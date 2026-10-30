// Package supervisor runs the loop: wake the agent for a turn, take the
// schedule it asked for, poll the core cheaply for edges in between, keep the
// heartbeat and the daily token ledger, and stop cleanly on SIGTERM. It does
// no triage of its own.
package supervisor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/triageagent-dev/spot/internal/agent"
	"github.com/triageagent-dev/spot/internal/config"
	"github.com/triageagent-dev/spot/internal/corecli"
	"github.com/triageagent-dev/spot/internal/edges"
	"github.com/triageagent-dev/spot/internal/ledger"
	"github.com/triageagent-dev/spot/internal/prompt"
	"github.com/triageagent-dev/spot/internal/schedule"
	"github.com/triageagent-dev/spot/internal/tools"
	"github.com/triageagent-dev/spot/internal/triggers"
)

// Core is the supervisor's view of the core.
type Core interface {
	ListOpenIssues(ctx context.Context) ([]corecli.Issue, error)
	Heartbeat(ctx context.Context, id, title, content string, staleAfter time.Duration) error
	RoomMessages(ctx context.Context, room, since string) ([]corecli.RoomMessage, string, error)
	Typing(ctx context.Context, room string, on bool) error
	Identity(ctx context.Context) (corecli.Identity, error)
	// StandingNotes are the spot's own notes and the rules people gave it, one
	// line each, rules first, and whether a rule says to check nothing until
	// a person asks.
	StandingNotes(ctx context.Context, spot string) (lines []string, hold bool, err error)
}

// Turner runs one turn of the agent. Every turn is an empty conversation.
type Turner interface {
	Run(ctx context.Context, t agent.Turn) agent.Result
}

// Interjector is a Turner that can take a message while a turn is running. The
// agent is one; a Turner that is not never sees one, and a message that
// arrives during its turn waits for the turn to end.
type Interjector interface {
	// Inject reports whether the message was handed to a running turn.
	Inject(text string) bool
}

// Supervisor owns the loop. Create it with New and call Run once.
type Supervisor struct {
	// startedAt is when Run began: a substrate process with no room cursor counts
	// what was said just before it as news (startGrace).
	startedAt time.Time
	cfg       config.Config
	led       *ledger.Ledger
	core      Core
	turner    Turner
	// fast is the second, short lane: while the turner runs a long turn, a
	// person's message and a wake the spot promised run on it, in a turn of
	// their own with its own tool state, instead of waiting or interrupting.
	// Nil = no second lane. side is the turn running on it, if any.
	fast Turner
	side *sideTurn
	// readNext is set when the stream said, during a turn, that the core's
	// issues or everything may have changed: the next wait reads the core at
	// once instead of sleeping for a frame the supervisor has already taken.
	readNext bool
	log      *slog.Logger
	now      func() time.Time

	typingEvery time.Duration // zero = typingEvery; tests shorten it

	tracker edges.Tracker
	held    []heldEdge
	rooms   map[string]*roomState

	trig        Trigger       // the core's push channel; nil = poll
	chatName    string        // what the core says the messenger calls the spot; empty = not known
	identityAt  time.Time     // when it was last read
	triggerWait time.Duration // zero = triggerDebounce; tests shorten it

	// nextTick is the planned wake. With the stream up the core sends it when it
	// is due (order); the spot wakes itself only as a net under a core that does
	// not (selfWakeAt).
	nextTick     time.Time
	scheduledWhy string
	flush        func()          // runs when the spot goes idle (SetFlush)
	order        *triggers.Order // a wake the core asked for, not yet taken
	manual       *triggers.Order // a wake a person asked for from the console, not yet taken
	// promise is the earliest wake the agent itself asked for that has not
	// fired. A later turn that wants a later (or no) wake does not move it.
	promise    time.Time
	promiseWhy string
	// promiseHuman is set when the promised wake answers a person's request for
	// later: it runs even under a hold rule, and it survives a restart.
	promiseHuman bool
	// digest is the spot's own summary of each of its last wakes, the only
	// thing carried from one wake to the next (the conversation is not).
	digest      []ledger.DigestEntry
	lastTurnEnd time.Time
	failures    int
	edgeWakes   []time.Time // early wakes in the last hour
	status      Status

	// recent is the tokens each turn of the last hour billed (hourlyUntil).
	recent []tokenAt

	// The wakes in hand (the table the core's controls act on) and the tasks that
	// run beside the lanes, each in a goroutine of its own: taskc brings their
	// ends back to the loop, which does the accounting.
	work    work
	taskc   chan taskEnd
	newTask func() Turner
	twg     sync.WaitGroup

	// What the core is told about how the spot is doing (counted per UTC day).
	statsDay string
	wakes    map[string]int64
	statsAt  time.Time
	coreHeld bool // the core's cap holds the spot back
}

type heldEdge struct {
	edges.Edge
	at time.Time
	// retries is how many times a wake that carried this edge failed before the
	// model saw it and the edge was put back.
	retries int
}

type roomState struct {
	seenTS   int64        // newest message the polls have seen (ms)
	readTS   atomic.Int64 // what the last finished turn could see; read_workspace_room asks for newer only
	cursor   string
	primed   bool
	disabled time.Time // do not ask again before this
}

// Status is /state/supervisor/status.json.
type Status struct {
	Name        string `json:"name"`
	Tenant      string `json:"tenant"`
	State       string `json:"state"` // starting, idle, running, paused-ceiling, stopping, stopped
	StartedAt   string `json:"startedAt"`
	UpdatedAt   string `json:"updatedAt"`
	LastTurn    *Turn  `json:"lastTurn,omitempty"`
	NextWake    *Next  `json:"nextWake,omitempty"`
	Tokens      Tokens `json:"tokensToday"`
	LastError   string `json:"lastError,omitempty"`
	Failures    int    `json:"consecutiveFailures,omitempty"`
	CoreError   string `json:"coreError,omitempty"`
	HeartbeatAt string `json:"heartbeatAt,omitempty"`
	HeartbeatID string `json:"heartbeatId,omitempty"`
	// HeldEdges is how many changes wait for the next wake; EdgeWakesHour how
	// many early wakes ran in the last hour against the cap.
	// Triggers is how the core is watched: "stream" (it tells the spot) or
	// "poll" (the spot asks every poll interval, as before the stream is up).
	Triggers      string `json:"triggers,omitempty"`
	HeldEdges     int    `json:"heldEdges,omitempty"`
	EdgeWakesHour int    `json:"edgeWakesLastHour,omitempty"`
}

// Turn summarises the last turn.
type Turn struct {
	At        string  `json:"at"`
	Reason    string  `json:"reason"`
	Seconds   float64 `json:"seconds"`
	Tokens    int64   `json:"tokens"`
	Steps     int     `json:"steps,omitempty"`
	ToolCalls int     `json:"toolCalls,omitempty"`
	Model     string  `json:"model,omitempty"`
	// Summary is the line the agent gave to finish.
	Summary        string `json:"summary,omitempty"`
	IsError        bool   `json:"isError"`
	StepsExhausted bool   `json:"stepsExhausted,omitempty"`
	TimedOut       bool   `json:"timedOut,omitempty"`
	AuthFailed     bool   `json:"authFailed,omitempty"`
	CoreDown       bool   `json:"coreDown,omitempty"`
	// ContextTokens is the conversation size at the turn's last request.
	ContextTokens int64 `json:"contextTokens,omitempty"`
}

// Next is the scheduled wake.
type Next struct {
	At     string `json:"at"`
	Why    string `json:"why,omitempty"`
	Source string `json:"source"`
	Note   string `json:"note,omitempty"`
}

// Tokens is the day's count.
type Tokens struct {
	Date      string `json:"date"`
	Used      int64  `json:"used"`
	Ceiling   int64  `json:"ceiling"`
	CacheRead int64  `json:"cacheRead"`
	Turns     int    `json:"turns"`
}

// New builds a Supervisor.
func New(cfg config.Config, led *ledger.Ledger, core Core, turner Turner, log *slog.Logger) *Supervisor {
	s := &Supervisor{
		cfg: cfg, led: led, core: core, turner: turner, log: log, now: time.Now,
		rooms: map[string]*roomState{"team": {}, "agents": {}},
		work:  work{running: map[string]*job{}}, taskc: make(chan taskEnd, 16),
	}
	// A saved cursor means the messages after it are news, restart or not.
	for room, ts := range led.LoadRoomRead() {
		if rs, ok := s.rooms[room]; ok {
			rs.readTS.Store(ts)
		}
	}
	for room, cur := range led.LoadRoomCursors() {
		if rs, ok := s.rooms[room]; ok && cur != "" {
			rs.cursor, rs.primed = cur, true
		}
	}
	s.startedAt = s.now()
	s.digest = led.LoadDigest()
	if cfg.Substrate() {
		s.loadWindow()
	}
	if pr := led.LoadPromise(); !pr.At.IsZero() {
		s.promise, s.promiseWhy, s.promiseHuman = pr.At, pr.Why, true
	}
	s.status = Status{Name: cfg.Name, Tenant: cfg.Tenant, HeartbeatID: cfg.HeartbeatID}
	return s
}

// SetFastTurner gives the spot its second lane (see Supervisor.fast).
func (s *Supervisor) SetFastTurner(t Turner) { s.fast = t }

// SetClock replaces the clock, for tests.
func (s *Supervisor) SetClock(f func() time.Time) { s.now = f }

// Trigger is the core's push channel (internal/triggers.Client): while it is
// connected the core says when to look, and the supervisor does not poll.
type Trigger interface {
	Connected() bool
	C() <-chan struct{}
	Take() triggers.Pending
	// Budget is the spot's token budget as the core last said it.
	Budget() (triggers.Budget, bool)
	// ReportTurn and ReportStats tell the core what a turn cost and how the
	// spot is doing.
	ReportTurn(triggers.Turn)
	ReportStats(triggers.Stats)
	// ReportWake tells the core one line of the spot's activity.
	ReportWake(triggers.Wake)
	// SetSchedule sets what the core is told of the schedule when the stream
	// (re)connects.
	SetSchedule(triggers.Schedule)
}

// SetTrigger switches the supervisor from polling to the core's stream. It
// still polls whenever the stream is down.
func (s *Supervisor) SetTrigger(t Trigger) { s.trig = t }

// triggerDebounce lets a burst finish before the core is read: the core
// forwards at most one state change a second and drops the rest, so the read
// after this wait sees them all.
const triggerDebounce = 1500 * time.Millisecond

type pending struct {
	reason      prompt.Reason
	facts       []string
	scheduledBy string
	note        string
	room        string // where the "typing…" indicator shows; empty = the agents room
	// edges are the held edges that made up the wake, so they can be put back if
	// the turn fails before the model saw them.
	edges []heldEdge
}

const maxHeld = 20

// Run blocks until ctx is cancelled. A killed pod starts fresh: the token
// ledger and the room cursors are in /state when it survives, and the rest is
// the core's.
func (s *Supervisor) Run(ctx context.Context) error {
	s.startedAt = s.now()
	s.status.StartedAt = s.startedAt.UTC().Format(time.RFC3339)
	s.setState("starting")

	s.refreshIdentity(ctx, true, 0)
	s.publishSchedule()
	pend := &pending{reason: prompt.Startup}
	if s.cfg.Substrate() {
		// An actor starts cold on every wake, and the core's request is the wake:
		// a startup wake would report a quiet plan over the one the core holds.
		pend = nil
		s.primeRooms(ctx)
	}
	for ctx.Err() == nil {
		s.drainTasks(ctx)
		if pend == nil {
			pend = s.manualWake()
		}
		if pend == nil {
			if pend = s.ordered(); pend != nil && s.cfg.Substrate() {
				pend = s.withRooms(ctx, pend)
			}
		}
		if pend == nil && s.selfWakeDue() {
			pend = &pending{reason: prompt.Tick, scheduledBy: s.scheduledWhy}
		}
		if pend != nil {
			if s.paused() {
				s.pauseForCeiling()
				s.reportWake(string(pend.reason), LaneLong, "ceiling", agent.Result{}, s.nextTick, "ceiling")
			} else {
				s.turn(ctx, pend)
				pend = nil
				continue
			}
		}

		read, alive := s.wait(ctx)
		if !alive {
			break
		}
		if read {
			if es := s.poll(ctx); len(es) > 0 {
				s.hold(es)
			}
		}
		if pend = s.dueFromHeld(); pend == nil && len(s.held) > 0 {
			s.setState(s.status.State) // the held count is visible while it waits
		}
	}
	s.setState("stopping")
	s.twg.Wait() // a task cut off by shutdown is still booked
	s.drainTasks(ctx)
	s.setState("stopped")
	return nil
}

// startGrace is how long before its start a substrate process still counts a
// message as news when it has no cursor: the message that wakes it is a few
// seconds old by the time the platform has resumed the actor.
const startGrace = 30 * time.Second

// primeRooms reads the rooms the process has no saved cursor for, so the cursor
// is in /state before the actor is suspended. What was said before the
// process started is history; what was said just before it is the wake. An actor lives a few seconds per wake and its poll runs every
// poll interval: without this a room is first read by the wake after the one
// that should have answered, and that read drops the message as history.
func (s *Supervisor) primeRooms(ctx context.Context) {
	var cold []string
	for room, rs := range s.rooms {
		if !rs.primed {
			cold = append(cold, room)
		}
	}
	if len(cold) == 0 {
		return
	}
	sort.Strings(cold)
	pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	s.hold(s.pollRooms(pctx, cold...))
}

// withRooms reads the rooms for a wake the core ordered from an actor. The
// core's request may have come with no usable token, which the actor answers
// with a plain tick that reads nothing of it: what was said in the rooms is
// then a fact of this wake, not of a wake that may never come.
func (s *Supervisor) withRooms(ctx context.Context, p *pending) *pending {
	pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	es := s.pollRooms(pctx, "team", "agents")
	if len(es) == 0 {
		return p
	}
	s.hold(es)
	h := s.fromHeld()
	h.scheduledBy = p.scheduledBy
	return h
}

// manualWake turns a wake a person asked for from the console into a wake of the
// long lane. It is never stale: the person asked for it now.
func (s *Supervisor) manualWake() *pending {
	m := s.manual
	s.manual = nil
	if m == nil {
		return nil
	}
	s.log.Info("a person asked for a wake from the console")
	return &pending{reason: prompt.Manual, scheduledBy: m.Why}
}

// manualSide starts the wake a person asked for on the short lane while the long
// one is busy. With no short lane, or one that is busy, it waits for the long
// turn to end.
func (s *Supervisor) manualSide(ctx context.Context) {
	if s.manual == nil || s.fast == nil || s.side != nil {
		return
	}
	if s.startSide(ctx, prompt.Manual, nil, nil, s.manual.Why) {
		s.log.Info("a wake a person asked for from the console runs on the short lane")
		s.manual = nil
	}
}

// orderSkew is how far ahead of the plan a wake the core asks for is still
// taken: the core's clock and the job that sends it are not exact.
const orderSkew = 30 * time.Second

// ordered turns the wake the core asked for into a wake of the long lane, or
// drops it when it is stale: a wake the spot ran meanwhile (its own net, an
// edge) already moved the plan, and a promise the spot no longer holds is gone.
func (s *Supervisor) ordered() *pending {
	o := s.order
	s.order = nil
	if o == nil {
		return nil
	}
	now := s.now()
	why := o.Why
	if s.cfg.Substrate() {
		// The core is the only clock of an actor, and a process that was started
		// for this wake holds neither the plan nor the promise: the core's order
		// is taken as it is.
		if o.Reason != "tick" && o.Reason != "promise" {
			s.log.Debug("a wake of a reason the spot does not know is dropped", "reason", o.Reason)
			return nil
		}
		s.log.Info("the core asks for a wake", "reason", o.Reason)
		return &pending{reason: prompt.Tick, scheduledBy: why}
	}
	switch o.Reason {
	case "promise":
		if s.promise.IsZero() || s.promise.After(now.Add(orderSkew)) {
			s.log.Debug("a promised wake the spot does not hold is dropped", "why", o.Why)
			return nil
		}
		if why == "" {
			why = s.promiseWhy
		}
	case "tick":
		if !s.nextTick.IsZero() && s.nextTick.After(now.Add(orderSkew)) {
			s.log.Debug("a tick the spot has planned later is dropped", "next", s.nextTick.Format(time.RFC3339))
			return nil
		}
		if why == "" {
			why = s.scheduledWhy
		}
	default:
		s.log.Debug("a wake of a reason the spot does not know is dropped", "reason", o.Reason)
		return nil
	}
	s.log.Info("the core asks for a wake", "reason", o.Reason)
	return &pending{reason: prompt.Tick, scheduledBy: why}
}

// selfWakeAt is when the spot wakes itself for the schedule. Where nobody else
// sends the tick (no stream is configured, or it is down and the core is polled)
// it is the planned time. With the stream up the core sends the tick, and the
// spot keeps no timer for it: it wakes itself only once maxWake has passed since
// its last wake, so a core that sends none never leaves it asleep for ever. A
// wake planned later than that (a ceiling) is not hurried.
func (s *Supervisor) selfWakeAt() time.Time {
	if s.nextTick.IsZero() || s.cfg.Substrate() {
		return time.Time{} // an actor has no timer: it is suspended when idle
	}
	if s.trig == nil || !s.trig.Connected() {
		return s.nextTick
	}
	if net := s.lastTurnEnd.Add(s.cfg.MaxWake); net.After(s.nextTick) {
		return net
	}
	return s.nextTick
}

func (s *Supervisor) selfWakeDue() bool {
	at := s.selfWakeAt()
	return !at.IsZero() && !s.now().Before(at)
}

// paused reports whether the daily ceiling, or the core's cap on the spot,
// holds the agent back.
func (s *Supervisor) paused() bool {
	_, hourly := s.hourlyUntil()
	return s.led.CeilingReached(s.cfg.DailyTokenCeil) || s.coreBlocked() || hourly
}

// tokenAt is what a turn billed and when it ended, for the hourly ceiling.
type tokenAt struct {
	at     time.Time
	tokens int64
}

// hourlyUntil says whether the tokens of the last hour are over the hourly
// ceiling and, if so, when the oldest turns that count have aged out enough for
// a wake to start. The samples are in memory: a restart starts the hour empty,
// the daily ceiling (written to disk) still holds.
func (s *Supervisor) hourlyUntil() (time.Time, bool) {
	ceil := s.cfg.HourlyTokenCeil
	if ceil <= 0 {
		return time.Time{}, false
	}
	now := s.now()
	keep := s.recent[:0]
	var sum int64
	for _, r := range s.recent {
		if now.Sub(r.at) < time.Hour {
			keep = append(keep, r)
			sum += r.tokens
		}
	}
	s.recent = keep
	if sum < ceil {
		return time.Time{}, false
	}
	for _, r := range s.recent {
		sum -= r.tokens
		if sum < ceil {
			return r.at.Add(time.Hour), true
		}
	}
	return now.Add(time.Hour), true
}

// coreBlocked is the core refusing the spot until UTC midnight: its own cap,
// which it holds on everything the spot does through it.
func (s *Supervisor) coreBlocked() bool {
	if s.trig == nil {
		return false
	}
	b, ok := s.trig.Budget()
	return ok && b.Blocked
}

func (s *Supervisor) pauseForCeiling() {
	if !s.led.CeilingReached(s.cfg.DailyTokenCeil) && !s.coreBlocked() {
		if until, ok := s.hourlyUntil(); ok {
			s.nextTick = until
			s.scheduledWhy = "the hourly token ceiling was reached; resumed at " + until.UTC().Format("15:04") + " UTC"
			s.log.Warn("hourly token ceiling reached; no wake until the hour has room", "until", until.Format(time.RFC3339), "ceiling", s.cfg.HourlyTokenCeil)
			s.status.NextWake = &Next{At: until.Format(time.RFC3339), Why: s.scheduledWhy, Source: "ceiling"}
			s.setState("paused-ceiling")
			return
		}
	}
	until := ledger.NextUTCMidnight(s.now())
	s.nextTick = until
	kind := "token"
	switch {
	case s.led.CeilingReached(s.cfg.DailyTokenCeil):
	default:
		kind = "core"
		s.coreHeld = true
	}
	s.scheduledWhy = "the daily " + kind + " ceiling was reached; resumed after UTC midnight"
	u := s.led.Usage()
	s.log.Warn("daily "+kind+" ceiling reached; no wake until UTC midnight", "until", until.Format(time.RFC3339),
		"usedTokens", u.Tokens, "tokenCeiling", s.cfg.DailyTokenCeil)
	s.status.NextWake = &Next{At: until.Format(time.RFC3339), Why: s.scheduledWhy, Source: "ceiling"}
	s.setState("paused-ceiling")
}

// wait sleeps until something needs a look and says whether to read the core
// (read) and whether the supervisor is still running (alive). With the stream
// up the core says when: a frame is what makes the supervisor read, and a
// timer only wakes it for its net under the core's ticks, for held edges to be judged again, and
// to notice that the stream went down. Without the stream it sleeps the poll
// interval and reads, as it always did.
func (s *Supervisor) wait(ctx context.Context) (read, alive bool) {
	if s.trig == nil || !s.trig.Connected() {
		s.setTriggers("poll")
		return true, s.sleepTasks(ctx, s.untilNext())
	}
	if s.readNext {
		s.readNext = false
		return true, true
	}
	s.setTriggers("stream")
	d := s.untilNext()
	if len(s.held) > 0 && d > time.Second {
		d = time.Second // judged again soon; no read of the core
	}
	// A spot that sleeps for hours still shows signs of life.
	s.reportStats(false)
	s.idle()
	if d > statsEvery {
		d = statsEvery
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false, false
	case <-t.C:
		return false, true
	case e := <-s.taskc:
		s.finishTask(ctx, e)
		return false, true
	case <-s.trig.C():
		wait := s.triggerWait
		if wait == 0 {
			wait = triggerDebounce
		}
		if !s.sleep(ctx, wait) {
			return false, false
		}
		p := s.trig.Take()
		s.takeCoreFacts(p)
		if p.Hello && p.Name == "" {
			s.refreshIdentity(ctx, true, 0) // an older core does not send the name: ask
		}
		return p.Wake(), true
	}
}

// statsEvery is how often the core hears from the spot while it sleeps.
const statsEvery = 2 * time.Minute

// takeCoreFacts applies what the core said about the spot itself: its name, its
// budget, and a wake it asks for. A spot the core had held back wakes at once when the cap lifts,
// instead of at midnight.
func (s *Supervisor) takeCoreFacts(p triggers.Pending) {
	if p.Manual != nil {
		s.manual = p.Manual
	}
	if o := p.Order; o != nil && (s.order == nil || o.Reason == "promise" || s.order.Reason != "promise") {
		s.order = o
	}
	if p.Name != "" && p.Name != s.chatName {
		s.log.Info("the messenger calls the spot", "name", p.Name)
		s.chatName = p.Name
	}
	if !p.Budget {
		return
	}
	if b, ok := s.trig.Budget(); ok && b.Own > 0 {
		if raised, err := s.led.RaiseTokens(b.Own); err != nil {
			s.log.Error("writing usage", "err", err)
		} else if raised {
			s.log.Info("the core counts more than this spot did: taking the core's number", "own", b.Own)
		}
	}
	held := s.coreBlocked()
	if held != s.coreHeld {
		b, _ := s.trig.Budget()
		s.log.Info("the core's cap on the spot changed", "blocked", held, "spent", b.Spent, "cap", b.Cap)
	}
	if s.coreHeld && !held {
		s.nextTick = s.now()
	}
	s.coreHeld = held
}

// RoomSince is the timestamp (ms) of the newest message of room the last
// finished turn could see; 0 when none is known.
func (s *Supervisor) RoomSince(room string) int64 {
	if rs, ok := s.rooms[room]; ok {
		return rs.readTS.Load()
	}
	return 0
}

// markRoomsRead records what the turn that just ended could see of each room,
// so the next wake reads only what is newer.
func (s *Supervisor) markRoomsRead() {
	m := map[string]int64{}
	for room, rs := range s.rooms {
		if rs.seenTS > 0 {
			rs.readTS.Store(rs.seenTS)
		}
		if ts := rs.readTS.Load(); ts > 0 {
			m[room] = ts
		}
	}
	if err := s.led.SaveRoomRead(m); err != nil {
		s.log.Warn("saving room watermarks", "err", err)
	}
}

// digestKeep is how many wakes the digest holds; digestLine clips one summary.
const (
	digestKeep = 4
	digestLine = 500
)

// digestLines renders the digest for the wake prompt.
func (s *Supervisor) digestLines() []string {
	out := make([]string, 0, len(s.digest))
	for _, d := range s.digest {
		line := d.At + " (" + d.Reason + "): " + d.Summary
		if d.Next != "" {
			line += " Next: " + d.Next
		}
		out = append(out, line)
	}
	return out
}

// addDigest records the spot's own summary of the wake that just ended.
func (s *Supervisor) addDigest(reason prompt.Reason, res agent.Result) {
	sum := strings.Join(strings.Fields(res.Summary), " ")
	if sum == "" {
		sum = "no summary: the wake ended at a cap or without finish."
	}
	if len(sum) > digestLine {
		sum = strings.ToValidUTF8(sum[:digestLine], "") + "…"
	}
	s.digest = append(s.digest, ledger.DigestEntry{At: s.now().UTC().Format("15:04Z"), Reason: string(reason), Summary: sum, Next: s.scheduledWhy})
	if len(s.digest) > digestKeep {
		s.digest = s.digest[len(s.digest)-digestKeep:]
	}
	if err := s.led.SaveDigest(s.digest); err != nil {
		s.log.Error("writing digest", "err", err)
	}
}

// setPromise records the earliest wake the agent promised itself; human says it
// answers a person's request, and only those are kept on disk.
func (s *Supervisor) promiseNow() schedule.Promise {
	return schedule.Promise{At: s.promise, Why: s.promiseWhy, Human: s.promiseHuman}
}

func (s *Supervisor) setPromise(at time.Time, why string, human bool) {
	s.promise, s.promiseWhy, s.promiseHuman = at, why, human
	pr := ledger.Promise{}
	if human {
		pr = ledger.Promise{At: at, Why: why}
	}
	if err := s.led.SavePromise(pr); err != nil {
		s.log.Error("writing the promised wake", "err", err)
	}
}

// holdWake ends a wake that a hold rule forbids, without a model call: what
// woke it is dropped, and the next wake is the longest the spot allows.
func (s *Supervisor) holdWake(p *pending) {
	s.log.Info("a hold rule is active: no model call", "reason", p.reason, "dropped", len(p.facts))
	s.idleWake(string(p.reason), "a hold rule is active", "hold")
}

// quietWake ends a wake with nothing to work on, without a model call.
func (s *Supervisor) quietWake(p *pending) {
	s.log.Info("nothing to act on: no facts, no open issue; no model call", "reason", p.reason)
	s.idleWake(string(p.reason), "nothing to act on: waiting for an event", "quiet")
}

func (s *Supervisor) idleWake(reason, why, source string) {
	s.lastTurnEnd = s.now()
	s.nextTick = s.now().Add(s.cfg.MaxWake)
	s.scheduledWhy = why
	if s.promiseHuman && !s.promise.IsZero() && s.promise.Before(s.nextTick) {
		s.nextTick, s.scheduledWhy = s.promise, s.promiseWhy
	}
	s.status.NextWake = &Next{At: s.nextTick.UTC().Format(time.RFC3339), Why: s.scheduledWhy, Source: source}
	s.reportWake(reason, LaneLong, source, agent.Result{}, s.nextTick, source)
	s.setState("idle")
}

// reportWake tells the core one line of the spot's activity, when a wake ends:
// the turn's counts, or zeros for a wake that made no model call. Labels and
// counts only, never what was said.
// maxLabel is the longest reason the core keeps on a line of activity.
const maxLabel = 24

// clipLabel cuts a label to maxLabel bytes at a character boundary.
func clipLabel(s string) string { return clipBytes(s, maxLabel) }

// clipBytes cuts s to n bytes at a character boundary.
func clipBytes(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return strings.ToValidUTF8(s[:n], "")
}

func (s *Supervisor) reportWake(reason, lane, outcome string, res agent.Result, next time.Time, nextSource string) {
	s.reportWakeAs("", "", reason, lane, outcome, res, next, nextSource)
}

// reportWakeAs is reportWake for a task's wake: key is the one its task list
// gives it, and title is the task's own. A wake with a key never touches the
// schedule the core keeps.
func (s *Supervisor) reportWakeAs(key, title, reason, lane, outcome string, res agent.Result, next time.Time, nextSource string) {
	if s.trig == nil {
		return
	}
	w := triggers.Wake{
		TS: s.now().UnixMilli(), Reason: clipLabel(reason), Lane: lane, Outcome: outcome,
		DurationMs: res.Duration.Milliseconds(), Steps: int64(res.Steps), ToolCalls: int64(res.ToolCalls),
		Billable: res.Usage.Billable(), CacheRead: res.Usage.CacheRead, Output: res.Usage.Output, NextSource: nextSource,
		Key: clipBytes(key, triggers.MaxTaskKey), Title: clipBytes(title, triggers.MaxTitle),
	}
	if lane != LaneTask {
		// The core keeps the schedule as the last wake of the long or the short lane
		// reported it, so each says all of it: the plan as it stands, and the promise.
		if next.IsZero() {
			next, w.NextSource = s.nextTick, ""
			if n := s.status.NextWake; n != nil {
				w.NextSource = clipLabel(n.Source)
			}
		}
		w.NextWhy, w.Promise = clipBytes(s.scheduledWhy, maxWhy), s.promiseReport()
		s.trig.SetSchedule(s.schedule())
	}
	if !next.IsZero() {
		w.Next = next.UnixMilli()
	}
	for _, st := range res.Trace {
		w.Trace = append(w.Trace, triggers.Step{Kind: st.Kind, Name: st.Name, Ms: st.Ms, Tokens: st.Tokens})
	}
	s.trig.ReportWake(w)
}

// maxWhy is the longest reason the core is told with a wake.
const maxWhy = 200

// promiseReport is the promised wake as the core is told it, nil when there is
// none.
func (s *Supervisor) promiseReport() *triggers.Promise {
	if s.promise.IsZero() {
		return nil
	}
	return &triggers.Promise{At: s.promise.UnixMilli(), Why: clipBytes(s.promiseWhy, maxWhy), Human: s.promiseHuman}
}

// schedule is what the spot has planned, as the core is told it when the stream
// (re)connects.
func (s *Supervisor) schedule() triggers.Schedule {
	sc := triggers.Schedule{MaxWakeMs: s.cfg.MaxWake.Milliseconds(), Promise: s.promiseReport()}
	if !s.nextTick.IsZero() {
		sc.Next, sc.NextWhy = s.nextTick.UnixMilli(), clipBytes(s.scheduledWhy, maxWhy)
		if n := s.status.NextWake; n != nil {
			sc.NextSource = clipLabel(n.Source)
		}
	}
	if s.failures > 0 {
		sc.BackoffMs = s.backoff().Milliseconds()
	}
	return sc
}

// publishSchedule hands the client the schedule it answers a hello with.
func (s *Supervisor) publishSchedule() {
	if s.trig != nil {
		s.trig.SetSchedule(s.schedule())
	}
}

// turnOutcome is the label of a turn that ran: it went well or it failed.
func turnOutcome(failed bool) string {
	if failed {
		return "failed"
	}
	return "turn"
}

// quiet reports whether a wake has nothing to work on: a startup or a timer
// with no facts and no held edges, the core showing no open issue, and nobody (a
// person, or the agent for itself) having asked for this wake. It then costs no
// model call: every step of a turn sends the whole prompt again, and a spot that
// is woken for nothing would read its notes and go looking. An event, a message
// or the wake the agent asked for still runs a turn.
func (s *Supervisor) quiet(ctx context.Context, reason prompt.Reason, p *pending, asked bool) bool {
	if !s.cfg.SkipQuietWakes || asked || (reason != prompt.Startup && reason != prompt.Tick) || len(p.facts) > 0 || len(p.edges) > 0 {
		return false
	}
	qctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	issues, err := s.core.ListOpenIssues(qctx)
	return err == nil && len(issues) == 0
}

// currentFacts drops what the wake need not show: the same fact twice, and a
// fact about an issue that is no longer open.
func (s *Supervisor) currentFacts(ctx context.Context, p *pending) []string {
	gone := map[string]bool{}
	var ids []string
	for _, e := range p.edges {
		if e.Kind != edges.Message && e.IssueID != "" {
			ids = append(ids, e.IssueID)
		}
	}
	if len(ids) > 0 {
		pctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		issues, err := s.core.ListOpenIssues(pctx)
		cancel()
		if err == nil {
			open := map[string]bool{}
			for _, is := range issues {
				open[is.ID] = true
			}
			for _, e := range p.edges {
				if e.Kind != edges.Message && e.IssueID != "" && !open[e.IssueID] {
					gone[e.Fact] = true
				}
			}
		}
	}
	var out []string
	seen := map[string]bool{}
	for _, f := range p.facts {
		if gone[f] || seen[f] {
			continue
		}
		seen[f] = true
		out = append(out, f)
	}
	return out
}

// identityEvery is how long the name the core gave the spot is trusted before
// it is read again: the workspace's owner can change it at any time.
const identityEvery = 5 * time.Minute

// identityGap is the least time between two reads of the name that a message
// can cause.
const identityGap = 20 * time.Second

// messengerName is what the messenger calls the spot: what the core last said,
// else the configured chat name; empty when neither is known.
func (s *Supervisor) messengerName() string {
	if s.chatName != "" {
		return s.chatName
	}
	return s.cfg.ChatName
}

// refreshIdentity reads the spot's name from the core, when forced or when the
// last read is old. A core without the tool, or one that cannot be reached, is
// not an error: the configured chat name stands.
func (s *Supervisor) refreshIdentity(ctx context.Context, force bool, maxAge time.Duration) {
	if !force && s.now().Sub(s.identityAt) < maxAge {
		return
	}
	s.identityAt = s.now()
	ictx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	id, err := s.core.Identity(ictx)
	if err != nil {
		if !errors.Is(err, corecli.ErrNoIdentityTool) && ctx.Err() == nil {
			s.log.Debug("reading the messenger name", "err", err)
		}
		return
	}
	if id.SpotName != "" && id.SpotName != s.chatName {
		s.log.Info("the messenger calls the spot", "name", id.SpotName)
		s.chatName = id.SpotName
	}
}

func (s *Supervisor) setTriggers(mode string) {
	if s.status.Triggers == mode {
		return
	}
	s.status.Triggers = mode
	s.log.Info("watching the core", "by", mode)
	s.setState(s.status.State)
	s.reportStats(true)
}

func (s *Supervisor) untilNext() time.Duration {
	d := s.cfg.PollInterval
	if at := s.selfWakeAt(); !at.IsZero() {
		if until := at.Sub(s.now()); until < d {
			d = until
		}
	}
	if d < 0 {
		d = 0
	}
	return d
}

// sleepTasks is sleep that also accounts the tasks that end while it waits.
func (s *Supervisor) sleepTasks(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
			return true
		case e := <-s.taskc:
			s.finishTask(ctx, e)
		}
	}
}

func (s *Supervisor) sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// hold keeps edges for the next wake. Past maxHeld, machine messages go
// first, then the oldest of the rest: a person's words are the last thing
// dropped.
func (s *Supervisor) hold(add []edges.Edge) {
	now := s.now()
	kept := make([]heldEdge, 0, len(add))
	for _, e := range add {
		kept = append(kept, heldEdge{Edge: e, at: now})
	}
	s.holdKept(kept)
}

// maxRetries is how many times an edge is put back after a wake that carried it
// failed before the model saw it. The next failure drops it, so a persistent
// failure cannot keep one fact alive for ever.
const maxRetries = 3

// requeue puts back the edges of a wake whose turn failed before any model
// request, so the next wake carries them. An edge put back maxRetries times is
// dropped, with a log line that names its kind and not its text.
func (s *Supervisor) requeue(edges []heldEdge) {
	var back []heldEdge
	for _, e := range edges {
		e.retries++
		if e.retries > maxRetries {
			s.log.Warn("an edge was dropped after its wake failed before the model saw it, too many times", "kind", e.Kind, "retries", maxRetries)
			continue
		}
		back = append(back, e)
	}
	if len(back) > 0 {
		s.log.Info("the turn failed before the model saw the wake; its facts are held for the next wake", "count", len(back))
		s.holdKept(back)
	}
}

func (s *Supervisor) holdKept(add []heldEdge) {
	s.held = append(s.held, add...)
	for len(s.held) > maxHeld {
		drop := -1
		for i, h := range s.held {
			if h.Kind == edges.Message && !h.Human {
				drop = i
				break
			}
		}
		if drop < 0 {
			drop = 0
		}
		s.held = append(s.held[:drop], s.held[drop+1:]...)
	}
	s.status.HeldEdges = len(s.held)
}

// liveGap is the shortest time between the end of a turn and the next one
// when a person's message is waiting, so a conversation does not stall on the
// edge cooldown.
const liveGap = 5 * time.Second

// dueFromHeld decides whether what is held wakes the agent now. Issue edges
// and messages that name the spot wake after the edge cooldown (or the
// failure backoff, whichever is longer). A person's message that does not
// name the spot waits up to ChatBatch. A machine's message never wakes on
// its own; it is a fact for the next wake. Early wakes are capped per hour.
func (s *Supervisor) dueFromHeld() *pending {
	if len(s.held) == 0 || s.paused() {
		return nil
	}
	now := s.now()
	gap := s.cfg.EdgeCooldown
	// A person who is talking to the spot is waiting for an answer: the gap
	// that spaces out machine edges is cut for a message that wakes it.
	for _, h := range s.held {
		if h.Kind == edges.Message && h.Human && (h.Mentioned || now.Sub(h.at) >= s.cfg.ChatBatch) && gap > liveGap {
			gap = liveGap
			break
		}
	}
	if s.failures > 0 {
		if b := s.backoff(); b > gap {
			gap = b
		}
	}
	if now.Sub(s.lastTurnEnd) < gap {
		return nil
	}
	urgent := false
	for _, h := range s.held {
		switch {
		case h.Kind != edges.Message:
			urgent = s.cfg.IssueWakes
		case h.Mentioned:
			urgent = true
		case h.Human && now.Sub(h.at) >= s.cfg.ChatBatch:
			urgent = true
		}
		if urgent {
			break
		}
	}
	if !urgent {
		return nil
	}
	if s.edgeWakesCapped(now) {
		return nil
	}
	s.edgeWakes = append(s.edgeWakes, now)
	s.status.EdgeWakesHour = len(s.edgeWakes)
	return s.fromHeld()
}

func (s *Supervisor) edgeWakesCapped(now time.Time) bool {
	kept := s.edgeWakes[:0]
	for _, t := range s.edgeWakes {
		if now.Sub(t) < time.Hour {
			kept = append(kept, t)
		}
	}
	s.edgeWakes = kept
	s.status.EdgeWakesHour = len(kept)
	if s.cfg.MaxEdgeWakesHr > 0 && len(kept) >= s.cfg.MaxEdgeWakesHr {
		s.log.Warn("early wakes capped for this hour; changes wait for the scheduled wake", "wakes", len(kept), "cap", s.cfg.MaxEdgeWakesHr, "held", len(s.held))
		return true
	}
	return false
}

func (s *Supervisor) fromHeld() *pending {
	p := &pending{reason: prompt.Edge, edges: append([]heldEdge(nil), s.held...)}
	for _, e := range s.held {
		p.facts = append(p.facts, e.Fact)
		if e.Human {
			p.reason = prompt.Human
		}
		// A person waits in the team room; machines talk in the agents room.
		switch {
		case e.Human:
			p.room = "team"
		case e.Room != "" && p.room == "":
			p.room = e.Room
		}
	}
	s.held = nil
	s.status.HeldEdges = 0
	return p
}

// poll is the model-free check of the core. It returns the new edges.
func (s *Supervisor) poll(ctx context.Context) []edges.Edge {
	// Whether a message names the spot depends on what it is called now.
	s.refreshIdentity(ctx, false, identityEvery)
	pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var out []edges.Edge

	issues, err := s.core.ListOpenIssues(pctx)
	if err != nil {
		if ctx.Err() == nil {
			s.noteCoreError("poll: " + err.Error())
		}
	} else {
		s.clearCoreError()
		out = append(out, s.tracker.Observe(issues)...)
	}

	out = append(out, s.pollRooms(pctx, "team", "agents")...)
	if len(out) > 0 {
		// Logs carry what happened, never what was said: the text of a person's
		// message or an issue is content, and is logged only on request.
		s.log.Info("edges", "count", len(out), "kinds", kindCounts(out), "human", anyHuman(out), "mentioned", anyMentioned(out))
		if s.cfg.LogContent {
			s.log.Debug("edge text", "first", clip(out[0].Fact, 200))
		}
	}
	return out
}

// pollRooms reads the given rooms and keeps where it got to.
func (s *Supervisor) pollRooms(ctx context.Context, rooms ...string) []edges.Edge {
	var out []edges.Edge
	moved := false
	for _, room := range rooms {
		es, changed := s.pollRoom(ctx, room)
		out = append(out, es...)
		moved = moved || changed
	}
	if moved {
		s.saveCursors()
	}
	return out
}

func (s *Supervisor) saveCursors() {
	c := map[string]string{}
	for room, rs := range s.rooms {
		if rs.cursor != "" {
			c[room] = rs.cursor
		}
	}
	if err := s.led.SaveRoomCursors(c); err != nil {
		s.log.Warn("saving room cursors", "err", err)
	}
}

// pollRoom reads one room. It returns the edges and whether the cursor moved.
func (s *Supervisor) pollRoom(ctx context.Context, room string) ([]edges.Edge, bool) {
	rs := s.rooms[room]
	if s.now().Before(rs.disabled) {
		return nil, false
	}
	msgs, next, err := s.core.RoomMessages(ctx, room, rs.cursor)
	switch {
	case errors.Is(err, corecli.ErrNoRoomTool):
		// The core predates ADR-095 phase 3 (the core's ADR). Ask again later, quietly.
		rs.disabled = s.now().Add(10 * time.Minute)
		s.log.Debug("core offers no read_workspace_room; skipping room polls")
		return nil, false
	case err != nil:
		if ctx.Err() == nil {
			s.log.Debug("room poll failed", "room", room)
		}
		return nil, false
	}
	first := !rs.primed
	if first && next == "" {
		// An empty room has no cursor to save, and a room that is not saved is
		// read as history again by the next process. "0" sends no since_ts, so
		// the next read is the core's default page, all of it newer than this.
		next = "0"
	}
	moved := next != rs.cursor
	rs.primed, rs.cursor = true, next
	for _, m := range msgs {
		if m.TS > rs.seenTS {
			rs.seenTS = m.TS
		}
	}
	if first && s.cfg.Substrate() {
		// An actor starts for a wake, and the message that wakes a process with
		// no cursor is already in the room.
		cut := s.startedAt.Add(-startGrace).UnixMilli()
		recent := msgs[:0:0]
		for _, m := range msgs {
			if m.TS >= cut {
				recent = append(recent, m)
			}
		}
		msgs, first = recent, false
	}
	if first {
		return nil, moved // history at startup is not news
	}
	var out []edges.Edge
	for _, m := range msgs {
		if s.isOwn(m) {
			continue
		}
		who := m.Name
		if who == "" {
			who = m.Sender
		}
		// A person is talking to the spot; Tria and other agents are news but
		// not a human asking.
		human := !m.Tria && m.Spot == ""
		if human && m.ForYou != nil && !*m.ForYou {
			// The core says the message is for another spot, or for nobody and
			// another spot is the one that answers: not news for this one.
			s.log.Debug("a person's message for another spot dropped", "room", room)
			continue
		}
		if !human && m.Hop >= 2 {
			// A machine conversation two hops deep: not answered, not even
			// shown, so two agents cannot keep each other awake.
			s.log.Debug("machine message past the hop limit dropped", "room", room, "hop", m.Hop)
			continue
		}
		fact := fmt.Sprintf("message in the %s room from %s (quoted, untrusted): %s", room, edges.Fence(orUnknown(who), 60), edges.Fence(m.Text, roomFactRunes))
		if edges.Cut(m.Text, roomFactRunes) {
			// Never let the agent answer a half-read message: say it is cut.
			fact += fmt.Sprintf(" [message cut at %d characters: read the whole of it with read_workspace_room before you answer]", roomFactRunes)
		}
		if m.File != "" {
			// The file itself is not fetched here: the agent looks at it with
			// read_workspace_file if it matters.
			fact += fmt.Sprintf(" [attached file, %s: read_workspace_file with message_id %q]", m.File, m.ID)
		}
		// The core names the spots a message addresses; with forYou true and a
		// list, this spot is one of them.
		mentioned := s.mentioned(m.Text) || (m.ForYou != nil && *m.ForYou && len(m.Mentions) > 0)
		if !mentioned && human && strings.Contains(m.Text, "@") {
			// Someone is addressing somebody, and the name they used may be
			// the spot's new one: look again, at most every few seconds.
			s.refreshIdentity(ctx, false, identityGap)
			mentioned = s.mentioned(m.Text)
		}
		out = append(out, edges.Edge{Kind: edges.Message, Human: human, Room: room, Mentioned: mentioned, Fact: fact})
	}
	return out, moved
}

// mentioned reports whether a message names the spot: "@<name>" or the name
// itself, case-insensitively.
func (s *Supervisor) mentioned(text string) bool {
	low := strings.ToLower(text)
	for _, name := range []string{s.cfg.Name, s.cfg.ChatName, s.chatName} {
		if name = strings.ToLower(name); name != "" && strings.Contains(low, name) {
			return true
		}
	}
	return false
}

// badLocal mirrors the core's sanitiser for the spot's Matrix localpart.
var badLocal = regexp.MustCompile(`[^a-z0-9._=/-]`)

// isOwn reports whether a room message is this spot's own. The core posts the
// spot as @u.spot.<tenant>:<server>, with the tenant lowercased and sanitised.
func (s *Supervisor) isOwn(m corecli.RoomMessage) bool {
	prefix := "@u.spot." + badLocal.ReplaceAllString(strings.ToLower(s.cfg.Tenant), "_") + ":"
	return strings.HasPrefix(m.Sender, prefix)
}

func orUnknown(s string) string {
	if s == "" {
		return "someone"
	}
	return s
}

func kindCounts(es []edges.Edge) string {
	n := map[edges.Kind]int{}
	for _, e := range es {
		n[e.Kind]++
	}
	keys := make([]string, 0, len(n))
	for k := range n {
		keys = append(keys, string(k))
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", k, n[edges.Kind(k)]))
	}
	return strings.Join(parts, ",")
}

func anyHuman(es []edges.Edge) bool {
	for _, e := range es {
		if e.Human {
			return true
		}
	}
	return false
}

func anyMentioned(es []edges.Edge) bool {
	for _, e := range es {
		if e.Mentioned {
			return true
		}
	}
	return false
}

// roomFactRunes is how much of a room message goes into the wake prompt; the
// rest is one read_workspace_room away.
const roomFactRunes = 2500

func clip(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

func (s *Supervisor) noteCoreError(msg string) {
	if s.status.CoreError == "" {
		s.log.Warn("core unreachable", "err", msg)
	}
	s.status.CoreError = msg
}

func (s *Supervisor) clearCoreError() {
	if s.status.CoreError != "" {
		s.log.Info("core reachable again")
		s.status.CoreError = ""
	}
}

// run is the turner's Run, with the loop kept going while it runs: a person
// who writes to the spot in the middle of a turn is handed to the running turn
// (interject) instead of waiting for it to end. The turn runs in its own
// goroutine so that everything the supervisor owns stays on this one.
func (s *Supervisor) run(ctx context.Context, t agent.Turn) agent.Result {
	inj, ok := s.turner.(Interjector)
	if !ok || s.trig == nil {
		return s.turner.Run(ctx, t)
	}
	done := make(chan agent.Result, 1)
	go func() { done <- s.turner.Run(ctx, t) }()
	var due <-chan time.Time
	if s.fast != nil {
		tk := time.NewTicker(s.sideEvery())
		defer tk.Stop()
		due = tk.C
	}
	for {
		select {
		case res := <-done:
			s.endSide(ctx) // a short turn still running is waited for: it is capped
			return res
		case res := <-s.sideDone():
			s.finishSide(ctx, res)
		case e := <-s.taskc:
			s.finishTask(ctx, e)
		case <-due:
			s.startPromised(ctx)
		case <-s.trig.C():
			s.interject(ctx, inj)
		}
	}
}

// sideEvery is how often a long turn looks for a promised wake that is due.
func (s *Supervisor) sideEvery() time.Duration {
	if s.cfg.PollInterval > 0 && s.cfg.PollInterval < time.Second {
		return s.cfg.PollInterval
	}
	return time.Second
}

// sideTurn is the short lane's turn while it runs.
type sideTurn struct {
	p    *pending
	done chan agent.Result
	inj  Interjector
	// talk are the edges the turn was started for, put back if it fails before
	// the model saw them.
	talk []edges.Edge
	job  *job
}

func (s *Supervisor) sideDone() <-chan agent.Result {
	if s.side == nil {
		return nil
	}
	return s.side.done
}

// startPromised starts the wake a person asked for when its time comes while a
// long turn is still running.
func (s *Supervisor) startPromised(ctx context.Context) {
	if s.side != nil || !s.promiseHuman || s.promise.IsZero() || s.now().Before(s.promise) {
		return
	}
	why := s.promiseWhy
	if s.startSide(ctx, prompt.Tick, nil, nil, why) {
		s.setPromise(time.Time{}, "", false) // it fired
	}
}

// startSide runs a short turn on the second lane. It reports false when there is
// none, one is running, or the daily ceiling holds the spot back.
func (s *Supervisor) startSide(ctx context.Context, reason prompt.Reason, facts []string, talk []edges.Edge, why string) bool {
	if s.fast == nil || s.side != nil || s.paused() {
		return false
	}
	rctx, rcancel := context.WithTimeout(ctx, 20*time.Second)
	rules, hold, rerr := s.core.StandingNotes(rctx, s.cfg.Name)
	rcancel()
	if rerr != nil && ctx.Err() == nil {
		s.log.Warn("standing notes not read; the short turn goes on without them", "err", rerr)
	}
	limited := hold && reason == prompt.Human
	text := prompt.Turn(prompt.Wake{
		Reason: reason, Now: s.now(), ChatName: s.messengerName(), Facts: facts, ScheduledBy: why,
		Digest: s.digestLines(), Rules: rules, Side: true, Limited: limited,
		TokensUsed: s.led.Usage().Tokens, TokensCeil: s.cfg.DailyTokenCeil,
	})
	j, jctx := s.begin(ctx, LaneShort, string(reason), tools.TaskRequest{})
	st := &sideTurn{p: &pending{reason: reason, facts: facts, scheduledBy: why}, done: make(chan agent.Result, 1), talk: talk, job: j}
	st.inj, _ = s.fast.(Interjector)
	s.side = st
	s.rollDay()
	s.wakes["side-"+string(reason)]++
	s.log.Info("short turn start", "reason", reason, "facts", len(facts))
	go func() {
		st.done <- s.fast.Run(jctx, agent.Turn{Reason: string(reason), Prompt: text, Limited: limited})
	}()
	return true
}

// endSide waits for the short turn when the long one ends, and books it.
func (s *Supervisor) endSide(ctx context.Context) {
	if s.side == nil {
		return
	}
	s.finishSide(ctx, <-s.side.done)
}

// finishSide books a short turn: its tokens, what it promised for later, its
// summary for the digest. The long turn's own end settles the schedule.
func (s *Supervisor) finishSide(ctx context.Context, res agent.Result) {
	st := s.side
	s.side = nil
	if stopped, _ := s.end(st.job); stopped && ctx.Err() == nil {
		res.Canceled = false // an operator ended it; the supervisor is not stopping
	}
	u := res.Usage
	s.book(res)
	failed := (res.Failed || res.CoreDown) && !res.Canceled
	if !res.Canceled {
		s.reportWake(string(st.p.reason), LaneShort, turnOutcome(failed), res, time.Time{}, "")
	}
	if res.Steps == 0 && failed && len(st.talk) > 0 {
		s.hold(st.talk)
	}
	for _, text := range res.Undelivered {
		s.hold([]edges.Edge{{Kind: edges.Message, Human: true, Mentioned: true, Room: "team", Fact: text}})
	}
	if res.Canceled {
		return
	}
	s.startTasks(ctx, res.Tasks)
	if failed {
		s.log.Error("short turn failed", "err", failureText(res, s.cfg.LLMAuth), "tokens", u.Billable())
		return
	}
	s.log.Info("short turn done", "reason", st.p.reason, "billable", u.Billable(), "read", u.CacheRead, "out", u.Output, "steps", res.Steps, "toolCalls", res.ToolCalls, "seconds", int(res.Duration.Seconds()))
	asked, after, why := res.Next != nil, "", ""
	if asked {
		after, why = res.Next.After, res.Next.Why
	}
	cur := s.promiseNow()
	if pr := schedule.AfterShortTurn(s.now(), s.cfg.MinWake, s.cfg.MaxWake, asked, after, why, cur); pr != cur {
		s.setPromise(pr.At, pr.Why, pr.Human)
	}
	s.addDigest(st.p.reason, res)
	s.markRoomsRead()
	s.reportStats(true)
}

// interject reads what the stream said during a turn. A person's message in a
// room goes to the running turn; a machine's is held for the next wake, as
// ever. The issues are not read here: the turn's own end reads them (what
// changed during it is held then), so a frame that only says they changed is
// kept for the loop's next read.
func (s *Supervisor) interject(ctx context.Context, inj Interjector) {
	wait := s.triggerWait
	if wait == 0 {
		wait = triggerDebounce
	}
	if !s.sleep(ctx, wait) {
		return
	}
	p := s.trig.Take()
	s.takeCoreFacts(p)
	s.manualSide(ctx)
	if p.Hello || p.Issues {
		s.readNext = true
	}
	if len(p.Rooms) == 0 {
		return
	}
	s.refreshIdentity(ctx, false, identityEvery)
	pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var talk, rest []edges.Edge
	for _, e := range s.pollRooms(pctx, p.Rooms...) {
		// What waits for a quiet moment in the normal loop (a person's message that
		// does not name the spot, while chat batching is on) waits here too.
		if e.Kind == edges.Message && e.Human && (e.Mentioned || s.cfg.ChatBatch <= 0) {
			talk = append(talk, e)
		} else {
			rest = append(rest, e)
		}
	}
	if len(rest) > 0 {
		s.hold(rest)
		s.log.Info("held during the turn", "count", len(rest), "kinds", kindCounts(rest))
	}
	if len(talk) == 0 {
		return
	}
	facts := make([]string, 0, len(talk))
	for _, e := range talk {
		facts = append(facts, e.Fact)
	}
	if s.fast != nil {
		// A person's message does not wait for the long turn and does not
		// interrupt it: the short lane takes it, or the short turn already
		// running does.
		if s.side != nil && s.side.inj != nil && s.side.inj.Inject(prompt.Interjection(s.messengerName(), facts)) {
			s.log.Info("passed to the running short turn", "count", len(talk), "mentioned", anyMentioned(talk))
			return
		}
		if s.startSide(ctx, prompt.Human, facts, talk, "") {
			s.log.Info("a person's message runs on the short lane", "count", len(talk), "mentioned", anyMentioned(talk))
			return
		}
	}
	if !inj.Inject(prompt.Interjection(s.messengerName(), facts)) {
		s.hold(talk) // the turn ended first: they wake the agent as usual
		s.log.Info("the running turn did not take the message; held for the next wake", "count", len(talk))
		return
	}
	s.log.Info("passed to the running turn", "count", len(talk), "mentioned", anyMentioned(talk))
	s.rollDay()
	s.wakes["interjected"]++
}

// turn runs one wake.
func (s *Supervisor) turn(ctx context.Context, p *pending) {
	sess := s.led.LoadSession()
	interrupted := sess.Running
	// Every wake is an empty conversation: nothing is resumed. What the spot
	// knows comes from the room and the issues it reads from the core at the
	// start of the wake and from the core's memory. A long conversation would
	// be paid for again on every model call.

	if len(s.held) > 0 && p.reason != prompt.Startup {
		for _, e := range s.held {
			p.facts = append(p.facts, e.Fact)
		}
		p.edges = append(p.edges, s.held...)
		s.held = nil
		s.status.HeldEdges = 0
	}
	used := s.led.Usage().Tokens
	s.refreshIdentity(ctx, false, identityEvery)
	rctx, rcancel := context.WithTimeout(ctx, 20*time.Second)
	rules, hold, rerr := s.core.StandingNotes(rctx, s.cfg.Name)
	rcancel()
	if rerr != nil && ctx.Err() == nil {
		s.log.Warn("standing notes not read; the wake goes on without them", "err", rerr)
	}
	if hold && p.reason != prompt.Human && p.reason != prompt.Manual && (p.reason != prompt.Tick || !s.promiseHuman) {
		// A person said to check nothing until they ask: no model call at all.
		s.holdWake(p)
		return
	}
	if p.reason == prompt.Tick && s.promiseHuman {
		// This wake is the promised one: the short lane must not run it again.
		s.setPromise(time.Time{}, "", false)
	}
	p.facts = s.currentFacts(ctx, p)
	if s.quiet(ctx, p.reason, p, p.scheduledBy != "" || s.promiseHuman) {
		s.quietWake(p)
		return
	}
	// A person writes while a rule says to check nothing until asked: the turn
	// has the notes, the room and the post tools only. A check they ask for is a
	// wake the spot schedules, and that one has every tool.
	limited := hold && p.reason == prompt.Human
	text := prompt.Turn(prompt.Wake{
		Limited: limited,
		Reason:  p.reason, Now: s.now(), ChatName: s.messengerName(), Facts: p.facts, ScheduledBy: p.scheduledBy,
		Interrupted: interrupted, Note: p.note, Digest: s.digestLines(), Rules: rules,
		TokensUsed: used, TokensCeil: s.cfg.DailyTokenCeil,
	})

	sess.Running = true
	if err := s.led.SaveSession(sess); err != nil {
		s.log.Error("saving session", "err", err)
	}
	s.setState("running")
	s.log.Info("turn start", "reason", p.reason, "facts", len(p.facts))
	s.rollDay()
	s.wakes[string(p.reason)]++

	pre := s.tracker.Snapshot()
	stopTyping := s.showWorking(ctx, p.room)
	wk, wctx := s.begin(ctx, LaneLong, string(p.reason), tools.TaskRequest{})
	res := s.run(wctx, agent.Turn{Reason: string(p.reason), Prompt: text, Room: p.room, Limited: limited})
	stopTyping()
	if stopped, _ := s.end(wk); stopped && ctx.Err() == nil {
		res.Canceled = false // an operator ended it; the supervisor is not stopping
		s.log.Info("turn stopped by an operator")
	}
	s.lastTurnEnd = s.now()

	// Accounting first: tokens are spent whether or not the turn succeeded.
	u := res.Usage
	s.book(res)

	// A cut-off or timed-out turn is reported on the next start: a change may
	// be half done.
	sess.Running = res.Canceled || res.TimedOut
	// Running out of steps is the work stopping, not failing.
	failed := (res.Failed || res.CoreDown) && !res.Canceled
	if err := s.led.SaveSession(sess); err != nil {
		s.log.Error("saving session", "err", err)
	}

	// A turn that failed before any model request never showed the wake to the
	// model: its facts (a person's message above all) go back to be carried by
	// the next wake. Shutdown is not this: the next start reports the cut-off.
	if res.Steps == 0 && (res.Failed || res.CoreDown) && !res.Canceled {
		s.requeue(p.edges)
	}

	// A message handed to the turn that it ended without showing to the model
	// wakes the agent next, as if it had arrived after the turn.
	for _, text := range res.Undelivered {
		s.hold([]edges.Edge{{Kind: edges.Message, Human: true, Mentioned: true, Room: "team", Fact: text}})
		s.log.Info("a message the turn did not take is held for the next wake")
	}

	s.status.LastTurn = s.lastTurn(p.reason, res, failed)

	if res.Canceled {
		s.log.Info("turn cut off by shutdown", "tokens", u.Billable())
		s.setState("stopping")
		return
	}
	s.startTasks(ctx, res.Tasks)

	if failed {
		s.failures++
		s.status.Failures = s.failures
		s.status.LastError = failureText(res, s.cfg.LLMAuth)
		s.log.Error("turn failed", "err", s.status.LastError, "failures", s.failures, "timedOut", res.TimedOut,
			"coreDown", res.CoreDown, "tokens", u.Billable())
	} else {
		s.failures, s.status.Failures, s.status.LastError = 0, 0, ""
		if res.StepsExhausted {
			s.log.Warn("turn stopped at max-steps; the work may be unfinished", "tokens", u.Billable(), "steps", res.Steps)
		}
		if res.ToolLimit {
			cap := "max-tool-calls"
			if res.TokenLimit {
				cap = "max-turn-tokens"
			}
			s.log.Warn("turn stopped at "+cap+"; the work may be unfinished", "tokens", u.Billable(), "toolCalls", res.ToolCalls)
		}
		s.log.Info("turn done", "billable", u.Billable(), "read", u.CacheRead, "out", u.Output, "steps", res.Steps, "toolCalls", res.ToolCalls, "seconds", int(res.Duration.Seconds()))
	}

	te := schedule.TurnEnd{
		Now: s.now(), Min: s.cfg.MinWake, Max: s.cfg.MaxWake,
		Human: p.reason == prompt.Human, Tick: p.reason == prompt.Tick,
		Failed: failed, Promise: s.promiseNow(),
	}
	if res.Next != nil {
		te.Asked, te.After, te.Why = true, res.Next.After, res.Next.Why
	}
	if failed {
		te.Backoff = s.backoff()
	}
	plan := schedule.AfterTurn(te)
	w := plan.Wake
	if w.Source != schedule.SourceAgent {
		s.log.Warn("schedule not taken as written", "source", w.Source, "note", w.Note, "after", w.After)
	}
	s.nextTick, s.scheduledWhy = plan.At, plan.Why
	s.setPromise(plan.Promise.At, plan.Promise.Why, plan.Promise.Human)
	s.status.NextWake = &Next{At: s.nextTick.UTC().Format(time.RFC3339), Why: w.Why, Source: string(w.Source), Note: w.Note}
	s.reportWake(string(p.reason), LaneLong, turnOutcome(failed), res, s.nextTick, string(w.Source))
	if !failed {
		s.addDigest(p.reason, res)
		s.markRoomsRead()
	}

	// Baseline after the turn. What changed during it, other than comments
	// (the agent writes those itself), is held and wakes the agent again by
	// the usual rules.
	if issues, err := s.core.ListOpenIssues(ctx); err == nil {
		if late := s.tracker.SettleFrom(pre, issues); len(late) > 0 {
			s.log.Info("changes during the turn", "count", len(late), "kinds", kindCounts(late))
			s.hold(late)
		}
	}

	if !failed {
		s.heartbeat(ctx, w, p.reason)
	} else {
		s.log.Warn("heartbeat not refreshed: the turn failed, so reflection stays the floor")
	}
	s.setState("idle")
	s.reportStats(true) // the turn is counted: the core hears it now, not at the next heartbeat
}

// typingEvery is how often the indicator is renewed; the core clears it after
// about 30 seconds without one.
const typingEvery = 20 * time.Second

// showWorking shows the spot's "typing…" indicator in room (the agents room
// when empty) while a turn runs, so people can see it is thinking, and
// returns the function that clears it. It is model-free and best effort: a
// core without the tool, or a failed call, only costs the indicator.
func (s *Supervisor) showWorking(ctx context.Context, room string) func() {
	if room == "" {
		room = "agents"
	}
	every := s.typingEvery
	if every <= 0 {
		every = typingEvery
	}
	set := func(c context.Context, on bool) bool {
		err := s.core.Typing(c, room, on)
		switch {
		case err == nil:
			return true
		case errors.Is(err, corecli.ErrNoTypingTool):
			s.log.Debug("core offers no set_workspace_typing; no typing indicator")
		case c.Err() == nil:
			s.log.Debug("typing indicator failed", "room", room, "err", err)
		}
		return false
	}
	if !set(ctx, true) {
		return func() {}
	}
	done, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				set(ctx, true)
			}
		}
	}()
	return func() {
		close(done)
		<-stopped
		// The turn's context may be gone (shutdown); clearing still matters.
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		set(c, false)
	}
}

func (s *Supervisor) backoff() time.Duration {
	d := s.cfg.MinWake
	for i := 1; i < s.failures && d < s.cfg.MaxWake; i++ {
		d *= 2
	}
	if d > s.cfg.MaxWake {
		d = s.cfg.MaxWake
	}
	return d
}

// federationHint is what to read when federation auth fails. The API answers
// every denied exchange with the same opaque 401; the reason is only in the
// Claude Console's authentication history.
const federationHint = "check the authentication history in the Claude Console (jti_reused / match_*)"

// looksLikeFederationFailure reports whether a turn's error reads like a
// failed token exchange, which the SDK may report without an API status.
func looksLikeFederationFailure(res agent.Result) bool {
	if res.AuthFailed {
		return true
	}
	if res.Err == nil {
		return false
	}
	text := strings.ToLower(res.Err.Error())
	for _, w := range []string{"authentication failed", "authentication_error", "workloadidentity", "jti_reused", "token exchange", "federation"} {
		if strings.Contains(text, w) {
			return true
		}
	}
	return false
}

// failureText describes why a turn failed, for the status file and the log.
// mode is the auth mode. The model's own words are never in it: res.Err holds
// what the API, the tools or the loop said.
func failureText(res agent.Result, mode string) string {
	if mode == config.AuthFederation && looksLikeFederationFailure(res) {
		return "Anthropic authentication failed in federation mode; " + federationHint
	}
	switch {
	case res.AuthFailed && mode == config.AuthVertex:
		return "Google rejected the credentials for Vertex AI (401 or 403); check the pod's Workload Identity and its roles"
	case res.AuthFailed:
		return "the Anthropic API rejected the key (401); check the key Secret"
	case res.CoreDown:
		return "the core MCP server was not reachable in this turn; check the core token and the core URL"
	case res.TimedOut:
		return "turn timed out"
	case res.Err != nil:
		return clip(res.Err.Error(), 300)
	default:
		return "the turn failed"
	}
}

func (s *Supervisor) heartbeat(ctx context.Context, w schedule.Wake, reason prompt.Reason) {
	stale := 2 * w.After
	if stale < 5*time.Minute {
		stale = 5 * time.Minute
	}
	u := s.led.Usage()
	content := fmt.Sprintf("%s is alive for tenant %s.\nlast turn: %s (%s)\nnext wake in %s: %s\ntokens today: %d of %s\n",
		s.cfg.Name, s.cfg.Tenant, s.now().UTC().Format(time.RFC3339), reason, w.After.Round(time.Second),
		orDash(w.Why), u.Tokens, ceilText(s.cfg.DailyTokenCeil))
	hctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := s.core.Heartbeat(hctx, s.cfg.HeartbeatID, "System-2 heartbeat: "+s.cfg.Name, content, stale); err != nil {
		s.log.Warn("heartbeat failed", "id", s.cfg.HeartbeatID, "err", err)
		s.noteCoreError("heartbeat: " + err.Error())
		return
	}
	s.status.HeartbeatAt = s.now().UTC().Format(time.RFC3339)
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func ceilText(c int64) string {
	if c <= 0 {
		return "no ceiling"
	}
	return fmt.Sprint(c)
}

// modelLabel is the model id the API reported, without a context-window suffix
// such as "[1m]"; "claude" when it reported none.
func modelLabel(m string) string {
	if i := strings.IndexByte(m, '['); i >= 0 {
		m = m[:i]
	}
	if m = strings.TrimSpace(m); m == "" {
		return "claude"
	}
	return m
}

// rollDay starts the day's counts afresh at UTC midnight.
func (s *Supervisor) rollDay() {
	day := s.now().UTC().Format("2006-01-02")
	if s.wakes == nil || day != s.statsDay {
		s.statsDay, s.wakes = day, map[string]int64{}
	}
}

// reportStats tells the core how the spot is doing: counts and labels, no
// content. At most every few seconds unless forced.
func (s *Supervisor) reportStats(force bool) {
	if s.trig == nil || (!force && s.now().Sub(s.statsAt) < 5*time.Second) {
		return
	}
	s.statsAt = s.now()
	s.rollDay()
	u := s.led.Usage()
	st := triggers.Stats{
		Version: s.cfg.Version, State: s.status.State, Turns: int64(u.Turns), Failures: int64(s.failures), Wakes: map[string]int64{},
		EdgeWakesHour: int64(s.status.EdgeWakesHour),
		Tokens:        u.Tokens, CacheReadTokens: u.CacheRead, OutputTokens: u.Output, Ceiling: s.cfg.DailyTokenCeil, Triggers: s.status.Triggers,
	}
	for k, v := range s.wakes {
		st.Wakes[k] = v
	}
	st.Config = s.reportedConfig()
	st.Tasks = s.taskLines()
	if s.status.State == "paused-ceiling" {
		st.Pause = "workspace-cap"
		if s.led.CeilingReached(s.cfg.DailyTokenCeil) {
			st.Pause = "own-ceiling"
		}
	}
	if t := s.status.LastTurn; t != nil {
		st.ContextTokens = t.ContextTokens
	}
	if n := s.status.NextWake; n != nil {
		if at, err := time.Parse(time.RFC3339, n.At); err == nil {
			st.NextWake = at.UnixMilli()
		}
	}
	s.trig.ReportStats(st)
}

// reportedConfig is the spot's limits and choices as the core is told them.
func (s *Supervisor) reportedConfig() *triggers.Config {
	c := s.cfg
	out := &triggers.Config{
		Provider: c.LLMProvider, Model: c.Model, Effort: c.Effort,
		MaxSteps: c.MaxSteps, MaxToolCalls: c.MaxToolCalls, MaxTurnTokens: c.MaxTurnTokens,
		MinWake: c.MinWake.String(), MaxWake: c.MaxWake.String(), ChatBatch: c.ChatBatch.String(), EdgeCooldown: c.EdgeCooldown.String(),
		MaxEdgeWakesHr: c.MaxEdgeWakesHr, CoreTools: c.CoreTools,
		RecallFirst: c.RecallFirst, PostGate: c.PostGate, FenceResults: c.FenceResults,
	}
	for _, k := range c.Connectors {
		out.Connectors = append(out.Connectors, triggers.Connector{Name: k.Name, Tools: k.Tools})
	}
	return out
}

func (s *Supervisor) setState(state string) {
	s.status.State = state
	s.status.UpdatedAt = s.now().UTC().Format(time.RFC3339)
	u := s.led.Usage()
	s.status.Tokens = Tokens{Date: u.Date, Used: u.Tokens, Ceiling: s.cfg.DailyTokenCeil, CacheRead: u.CacheRead, Turns: u.Turns}
	if err := s.led.WriteStatus(s.status); err != nil {
		s.log.Error("writing status", "err", err)
	}
	s.reportStats(false)
}

// book counts what a turn cost: in the ledger and, for the core, as the spot's
// own count (the core books it under the spot's kind and answers with the budget
// after it).
func (s *Supervisor) book(res agent.Result) {
	u := res.Usage
	if n := u.Billable(); n > 0 {
		s.recent = append(s.recent, tokenAt{at: s.now(), tokens: n})
	}
	if _, err := s.led.AddUsage(u.Billable(), u.CacheRead, u.Output); err != nil {
		s.log.Error("saving usage", "err", err)
	}
	if s.trig != nil && (u.Billable() > 0 || u.CacheRead > 0) {
		s.trig.ReportTurn(triggers.Turn{
			Model: modelLabel(res.Model), Input: u.Input + u.CacheWrite + u.CacheRead,
			Cached: u.CacheRead, Output: u.Output, DurationMs: res.Duration.Milliseconds(), Failed: res.Failed,
		})
	}
}

// lastTurn is the status file's line about the turn that just ended.
func (s *Supervisor) lastTurn(reason prompt.Reason, res agent.Result, failed bool) *Turn {
	return &Turn{
		At: s.now().UTC().Format(time.RFC3339), Reason: string(reason), Seconds: res.Duration.Seconds(),
		Tokens: res.Usage.Billable(), Steps: res.Steps, ToolCalls: res.ToolCalls, Model: res.Model, Summary: res.Summary,
		IsError: failed, StepsExhausted: res.StepsExhausted, TimedOut: res.TimedOut, AuthFailed: res.AuthFailed,
		CoreDown: res.CoreDown, ContextTokens: res.ContextTokens,
	}
}
