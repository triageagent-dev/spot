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
	"time"

	"github.com/triageagent-dev/tria-spot/internal/agent"
	"github.com/triageagent-dev/tria-spot/internal/config"
	"github.com/triageagent-dev/tria-spot/internal/corecli"
	"github.com/triageagent-dev/tria-spot/internal/edges"
	"github.com/triageagent-dev/tria-spot/internal/ledger"
	"github.com/triageagent-dev/tria-spot/internal/prompt"
	"github.com/triageagent-dev/tria-spot/internal/schedule"
	"github.com/triageagent-dev/tria-spot/internal/triggers"
)

// Core is the supervisor's view of the core.
type Core interface {
	ListOpenIssues(ctx context.Context) ([]corecli.Issue, error)
	Heartbeat(ctx context.Context, id, title, content string, staleAfter time.Duration) error
	RoomMessages(ctx context.Context, room, since string) ([]corecli.RoomMessage, string, error)
	Typing(ctx context.Context, room string, on bool) error
	Identity(ctx context.Context) (corecli.Identity, error)
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
	cfg    config.Config
	led    *ledger.Ledger
	core   Core
	turner Turner
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

	nextTick     time.Time
	scheduledWhy string
	lastTurnEnd  time.Time
	failures     int
	edgeWakes    []time.Time // early wakes in the last hour
	status       Status

	// What the core is told about how the spot is doing (counted per UTC day).
	statsDay string
	wakes    map[string]int64
	statsAt  time.Time
	coreHeld bool // the core's cap holds the spot back
}

type heldEdge struct {
	edges.Edge
	at time.Time
}

type roomState struct {
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
	}
	// A saved cursor means the messages after it are news, restart or not.
	for room, cur := range led.LoadRoomCursors() {
		if rs, ok := s.rooms[room]; ok && cur != "" {
			rs.cursor, rs.primed = cur, true
		}
	}
	s.status = Status{Name: cfg.Name, Tenant: cfg.Tenant, HeartbeatID: cfg.HeartbeatID}
	return s
}

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
}

const maxHeld = 20

// Run blocks until ctx is cancelled. A killed pod starts fresh: the token
// ledger and the room cursors are in /state when it survives, and the rest is
// the core's.
func (s *Supervisor) Run(ctx context.Context) error {
	s.status.StartedAt = s.now().UTC().Format(time.RFC3339)
	s.setState("starting")

	s.refreshIdentity(ctx, true, 0)
	pend := &pending{reason: prompt.Startup}
	for ctx.Err() == nil {
		if pend == nil && !s.nextTick.IsZero() && !s.now().Before(s.nextTick) {
			pend = &pending{reason: prompt.Tick, scheduledBy: s.scheduledWhy}
		}
		if pend != nil {
			if s.paused() {
				s.pauseForCeiling()
				pend = nil
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
	s.setState("stopped")
	return nil
}

// paused reports whether the daily ceiling, or the core's cap on the spot,
// holds the agent back.
func (s *Supervisor) paused() bool {
	return s.led.CeilingReached(s.cfg.DailyTokenCeil) || s.coreBlocked()
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
// timer only wakes it for the schedule, for held edges to be judged again, and
// to notice that the stream went down. Without the stream it sleeps the poll
// interval and reads, as it always did.
func (s *Supervisor) wait(ctx context.Context) (read, alive bool) {
	if s.trig == nil || !s.trig.Connected() {
		s.setTriggers("poll")
		return true, s.sleep(ctx, s.untilNext())
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

// takeCoreFacts applies what the core said about the spot itself: its name, and
// its budget. A spot the core had held back wakes at once when the cap lifts,
// instead of at midnight.
func (s *Supervisor) takeCoreFacts(p triggers.Pending) {
	if p.Name != "" && p.Name != s.chatName {
		s.log.Info("the messenger calls the spot", "name", p.Name)
		s.chatName = p.Name
	}
	if !p.Budget {
		return
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
	if !s.nextTick.IsZero() {
		if until := s.nextTick.Sub(s.now()); until < d {
			d = until
		}
	}
	if d < 0 {
		d = 0
	}
	return d
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
	for _, e := range add {
		s.held = append(s.held, heldEdge{Edge: e, at: now})
	}
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
			urgent = true
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
	p := &pending{reason: prompt.Edge}
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
		// The core predates ADR-095 phase 3. Ask again later, quietly.
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
	moved := next != rs.cursor
	rs.primed, rs.cursor = true, next
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
		mentioned := s.mentioned(m.Text)
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
	for {
		select {
		case res := <-done:
			return res
		case <-s.trig.C():
			s.interject(ctx, inj)
		}
	}
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
		s.held = nil
		s.status.HeldEdges = 0
	}
	used := s.led.Usage().Tokens
	s.refreshIdentity(ctx, false, identityEvery)
	text := prompt.Turn(prompt.Wake{
		Reason: p.reason, Now: s.now(), ChatName: s.messengerName(), Facts: p.facts, ScheduledBy: p.scheduledBy,
		Interrupted: interrupted, Note: p.note,
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
	res := s.run(ctx, agent.Turn{Reason: string(p.reason), Prompt: text, Room: p.room})
	stopTyping()
	s.lastTurnEnd = s.now()

	// Accounting first: tokens are spent whether or not the turn succeeded.
	u := res.Usage
	if _, err := s.led.AddUsage(u.Billable(), u.CacheRead, u.Output); err != nil {
		s.log.Error("saving usage", "err", err)
	}
	if s.trig != nil && (u.Billable() > 0 || u.CacheRead > 0) {
		// What the core is told is the spot's own count of this turn: the core
		// books it under the spot's kind and answers with the budget after it.
		s.trig.ReportTurn(triggers.Turn{
			Model: modelLabel(res.Model), Input: u.Input + u.CacheWrite + u.CacheRead,
			Cached: u.CacheRead, Output: u.Output, DurationMs: res.Duration.Milliseconds(),
			Failed: res.Failed,
		})
	}

	// A cut-off or timed-out turn is reported on the next start: a change may
	// be half done.
	sess.Running = res.Canceled || res.TimedOut
	// Running out of steps is the work stopping, not failing.
	failed := (res.Failed || res.CoreDown) && !res.Canceled
	if err := s.led.SaveSession(sess); err != nil {
		s.log.Error("saving session", "err", err)
	}

	// A message handed to the turn that it ended without showing to the model
	// wakes the agent next, as if it had arrived after the turn.
	for _, text := range res.Undelivered {
		s.hold([]edges.Edge{{Kind: edges.Message, Human: true, Mentioned: true, Room: "team", Fact: text}})
		s.log.Info("a message the turn did not take is held for the next wake")
	}

	turn := &Turn{
		At: s.lastTurnEnd.UTC().Format(time.RFC3339), Reason: string(p.reason), Seconds: res.Duration.Seconds(),
		Tokens: u.Billable(), Steps: res.Steps, ToolCalls: res.ToolCalls, Model: res.Model, Summary: res.Summary,
		IsError: failed, StepsExhausted: res.StepsExhausted, TimedOut: res.TimedOut, AuthFailed: res.AuthFailed,
		CoreDown: res.CoreDown, ContextTokens: res.ContextTokens,
	}
	s.status.LastTurn = turn

	if res.Canceled {
		s.log.Info("turn cut off by shutdown", "tokens", u.Billable())
		s.setState("stopping")
		return
	}

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
		s.log.Info("turn done", "billable", u.Billable(), "read", u.CacheRead, "out", u.Output, "steps", res.Steps, "toolCalls", res.ToolCalls, "seconds", int(res.Duration.Seconds()))
	}

	var w schedule.Wake
	if res.Next != nil {
		w = schedule.Request(res.Next.After, res.Next.Why, s.cfg.MinWake, s.cfg.MaxWake)
	} else {
		w = schedule.Default(s.cfg.MinWake, schedule.SourceMissing, "the agent did not call schedule_next")
	}
	if w.Source != schedule.SourceAgent {
		s.log.Warn("schedule not taken as written", "source", w.Source, "note", w.Note, "after", w.After)
	}
	if failed {
		if b := s.backoff(); b > w.After {
			w.After = b
		}
	}
	s.nextTick = s.now().Add(w.After)
	s.scheduledWhy = w.Why
	s.status.NextWake = &Next{At: s.nextTick.UTC().Format(time.RFC3339), Why: w.Why, Source: string(w.Source), Note: w.Note}

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
		State: s.status.State, Turns: int64(u.Turns), Failures: int64(s.failures), Wakes: map[string]int64{},
		EdgeWakesHour: int64(s.status.EdgeWakesHour),
		Tokens:        u.Tokens, CacheReadTokens: u.CacheRead, OutputTokens: u.Output, Ceiling: s.cfg.DailyTokenCeil, Triggers: s.status.Triggers,
	}
	for k, v := range s.wakes {
		st.Wakes[k] = v
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
