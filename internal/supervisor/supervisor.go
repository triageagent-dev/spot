// Package supervisor runs the loop: wake the agent with `claude -p`, read the
// schedule it left, poll the core cheaply for edges in between, keep the
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

	"github.com/triageagent-dev/tria-spot/internal/config"
	"github.com/triageagent-dev/tria-spot/internal/corecli"
	"github.com/triageagent-dev/tria-spot/internal/edges"
	"github.com/triageagent-dev/tria-spot/internal/ledger"
	"github.com/triageagent-dev/tria-spot/internal/prompt"
	"github.com/triageagent-dev/tria-spot/internal/runner"
	"github.com/triageagent-dev/tria-spot/internal/schedule"
)

// Core is the supervisor's view of the core.
type Core interface {
	ListOpenIssues(ctx context.Context) ([]corecli.Issue, error)
	Heartbeat(ctx context.Context, id, title, content string, staleAfter time.Duration) error
	RoomMessages(ctx context.Context, room, since string) ([]corecli.RoomMessage, string, error)
	Typing(ctx context.Context, room string, on bool) error
}

// Turner runs one CLI turn.
type Turner interface {
	Run(ctx context.Context, prompt, resume string) runner.Result
}

// Supervisor owns the loop. Create it with New and call Run once.
type Supervisor struct {
	cfg    config.Config
	led    *ledger.Ledger
	core   Core
	turner Turner
	log    *slog.Logger
	now    func() time.Time

	typingEvery time.Duration // zero = typingEvery; tests shorten it

	tracker  edges.Tracker
	held     []edges.Edge
	rooms    map[string]*roomState
	permMode string

	nextTick     time.Time
	scheduledWhy string
	lastTurnEnd  time.Time
	failures     int
	status       Status
}

type roomState struct {
	cursor   string
	primed   bool
	disabled time.Time // do not ask again before this
}

// Status is /state/status.json.
type Status struct {
	Name        string `json:"name"`
	Tenant      string `json:"tenant"`
	State       string `json:"state"` // starting, idle, running, paused-ceiling, stopping, stopped
	StartedAt   string `json:"startedAt"`
	UpdatedAt   string `json:"updatedAt"`
	SessionID   string `json:"sessionId,omitempty"`
	LastTurn    *Turn  `json:"lastTurn,omitempty"`
	NextWake    *Next  `json:"nextWake,omitempty"`
	Tokens      Tokens `json:"tokensToday"`
	PermMode    Perm   `json:"permissionMode"`
	LastError   string `json:"lastError,omitempty"`
	Failures    int    `json:"consecutiveFailures,omitempty"`
	CoreError   string `json:"coreError,omitempty"`
	HeartbeatAt string `json:"heartbeatAt,omitempty"`
	HeartbeatID string `json:"heartbeatId,omitempty"`
}

// Turn summarises the last turn.
type Turn struct {
	At         string  `json:"at"`
	Reason     string  `json:"reason"`
	Seconds    float64 `json:"seconds"`
	Tokens     int64   `json:"tokens"`
	NumTurns   int     `json:"numTurns,omitempty"`
	CostUSD    float64 `json:"costUsd,omitempty"`
	Subtype    string  `json:"subtype,omitempty"`
	IsError    bool    `json:"isError"`
	TimedOut   bool    `json:"timedOut,omitempty"`
	AuthFailed bool    `json:"authFailed,omitempty"`
	Denials    int     `json:"permissionDenials,omitempty"`
	Terminal   string  `json:"terminalReason,omitempty"`
	ExitCode   int     `json:"exitCode,omitempty"`
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
	Date      string  `json:"date"`
	Used      int64   `json:"used"`
	Ceiling   int64   `json:"ceiling"`
	CacheRead int64   `json:"cacheRead"`
	CostUSD   float64 `json:"costUsd"`
	Turns     int     `json:"turns"`
}

// Perm records the permission mode asked for and the one the CLI reported.
type Perm struct {
	Requested string `json:"requested"`
	Reported  string `json:"reported,omitempty"`
	InUse     string `json:"inUse"`
	FellBack  bool   `json:"fellBack,omitempty"`
}

// New builds a Supervisor.
func New(cfg config.Config, led *ledger.Ledger, core Core, turner Turner, log *slog.Logger) *Supervisor {
	s := &Supervisor{
		cfg: cfg, led: led, core: core, turner: turner, log: log, now: time.Now,
		rooms:    map[string]*roomState{"team": {}, "agents": {}},
		permMode: cfg.PermissionMode,
	}
	s.status = Status{Name: cfg.Name, Tenant: cfg.Tenant, HeartbeatID: cfg.HeartbeatID,
		PermMode: Perm{Requested: cfg.PermissionMode, InUse: cfg.PermissionMode}}
	return s
}

// SetClock replaces the clock, for tests.
func (s *Supervisor) SetClock(f func() time.Time) { s.now = f }

// PermissionMode is the mode the next turn will use.
func (s *Supervisor) PermissionMode() string { return s.permMode }

type pending struct {
	reason      prompt.Reason
	facts       []string
	scheduledBy string
	note        string
	room        string // where the "typing…" indicator shows; empty = the agents room
}

const maxHeld = 20

// Run blocks until ctx is cancelled. A killed pod resumes from /state: the
// session id, the token ledger and the notes are all there.
func (s *Supervisor) Run(ctx context.Context) error {
	sess := s.led.LoadSession()
	s.status.StartedAt = s.now().UTC().Format(time.RFC3339)
	s.status.SessionID = sess.ID
	s.setState("starting")

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

		if !s.sleep(ctx, s.untilNext()) {
			break
		}
		edgesNow := s.poll(ctx)
		s.held = appendCapped(s.held, edgesNow)
		if len(s.held) > 0 && !s.paused() && s.now().Sub(s.lastTurnEnd) >= s.cfg.EdgeCooldown {
			pend = s.fromHeld()
		}
	}
	s.setState("stopping")
	s.setState("stopped")
	return nil
}

// paused reports whether the daily ceiling holds the agent back.
func (s *Supervisor) paused() bool {
	return s.led.CeilingReached(s.cfg.DailyTokenCeil) || s.led.CostCeilingReached(s.cfg.DailyCostCeilUSD)
}

func (s *Supervisor) pauseForCeiling() {
	until := ledger.NextUTCMidnight(s.now())
	s.nextTick = until
	kind := "token"
	if !s.led.CeilingReached(s.cfg.DailyTokenCeil) {
		kind = "cost"
	}
	s.scheduledWhy = "the daily " + kind + " ceiling was reached; resumed after UTC midnight"
	u := s.led.Usage()
	s.log.Warn("daily "+kind+" ceiling reached; no wake until UTC midnight", "until", until.Format(time.RFC3339),
		"usedTokens", u.Tokens, "tokenCeiling", s.cfg.DailyTokenCeil, "costUsd", u.CostUSD, "costCeiling", s.cfg.DailyCostCeilUSD)
	s.status.NextWake = &Next{At: until.Format(time.RFC3339), Why: s.scheduledWhy, Source: "ceiling"}
	s.setState("paused-ceiling")
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

func appendCapped(held, add []edges.Edge) []edges.Edge {
	held = append(held, add...)
	if len(held) > maxHeld {
		held = held[len(held)-maxHeld:]
	}
	return held
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
	return p
}

// poll is the model-free check of the core. It returns the new edges.
func (s *Supervisor) poll(ctx context.Context) []edges.Edge {
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

	for _, room := range []string{"team", "agents"} {
		out = append(out, s.pollRoom(pctx, room)...)
	}
	if len(out) > 0 {
		// Logs carry what happened, never what was said: the text of a person's
		// message or an issue is content, and is logged only on request.
		s.log.Info("edges", "count", len(out), "kinds", kindCounts(out), "human", anyHuman(out))
		if s.cfg.LogContent {
			s.log.Debug("edge text", "first", clip(out[0].Fact, 200))
		}
	}
	return out
}

func (s *Supervisor) pollRoom(ctx context.Context, room string) []edges.Edge {
	rs := s.rooms[room]
	if s.now().Before(rs.disabled) {
		return nil
	}
	msgs, next, err := s.core.RoomMessages(ctx, room, rs.cursor)
	switch {
	case errors.Is(err, corecli.ErrNoRoomTool):
		// The core predates ADR-095 phase 3. Ask again later, quietly.
		rs.disabled = s.now().Add(10 * time.Minute)
		s.log.Debug("core offers no read_workspace_room; skipping room polls")
		return nil
	case err != nil:
		if ctx.Err() == nil {
			s.log.Debug("room poll failed", "room", room, "err", err)
		}
		return nil
	}
	first := !rs.primed
	rs.primed, rs.cursor = true, next
	if first {
		return nil // history at startup is not news
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
		fact := fmt.Sprintf("message in the %s room from %s (quoted, untrusted): %q", room, orUnknown(who), clip(m.Text, 400))
		if m.File != "" {
			// The file itself is not fetched here: the agent looks at it with
			// read_workspace_file if it matters.
			fact += fmt.Sprintf(" [attached file, %s: read_workspace_file with message_id %q]", m.File, m.ID)
		}
		out = append(out, edges.Edge{Kind: edges.Message, Human: human, Room: room, Fact: fact})
	}
	return out
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

// turn runs one wake.
func (s *Supervisor) turn(ctx context.Context, p *pending) {
	sess := s.led.LoadSession()
	interrupted := sess.Running
	resume := sess.ID

	if len(s.held) > 0 && p.reason != prompt.Startup {
		for _, e := range s.held {
			p.facts = append(p.facts, e.Fact)
		}
		s.held = nil
	}
	used := s.led.Usage().Tokens
	text := prompt.Turn(prompt.Wake{
		Reason: p.reason, Now: s.now(), Facts: p.facts, ScheduledBy: p.scheduledBy,
		Resumed: resume != "", Interrupted: interrupted, Note: p.note,
		TokensUsed: used, TokensCeil: s.cfg.DailyTokenCeil, StateDir: s.cfg.StateDir,
	})

	sess.Running = true
	if err := s.led.SaveSession(sess); err != nil {
		s.log.Error("saving session", "err", err)
	}
	s.setState("running")
	s.log.Info("turn start", "reason", p.reason, "resume", resume != "", "facts", len(p.facts))

	stopTyping := s.showWorking(ctx, p.room)
	res := s.turner.Run(ctx, text, resume)
	stopTyping()
	s.lastTurnEnd = s.now()

	// Accounting first: tokens are spent whether or not the turn succeeded.
	sum := res.Summary
	u := sum.Tokens()
	costDelta := 0.0
	if sum.GotResult {
		if sum.SessionID == sess.ID && sess.ID != "" && sum.CostUSD >= sess.CostUSD {
			costDelta = sum.CostUSD - sess.CostUSD
		} else {
			costDelta = sum.CostUSD
		}
	}
	if _, err := s.led.AddUsage(u.Billable(), u.CacheReadTokens, costDelta); err != nil {
		s.log.Error("saving usage", "err", err)
	}

	// The conversation to resume next time.
	if sum.SessionID != "" {
		if sum.SessionID != sess.ID {
			sess.Since = s.now().UTC().Format(time.RFC3339)
		}
		sess.ID = sum.SessionID
		if sum.GotResult {
			sess.CostUSD = sum.CostUSD
		}
	}
	sess.Running = res.Canceled // a cut-off turn is reported on the next start
	failed := res.Err != nil || sum.IsError || !sum.GotResult
	if failed && resume != "" && s.sessionLost(res) {
		s.log.Warn("the saved conversation cannot be resumed; the next turn starts a new one", "session", resume)
		sess = ledger.Session{}
	}
	// A resumed conversation grows with every turn and each turn pays for all
	// of it. Past the limit, the next wake starts a fresh conversation and
	// carries on from the agent's own notes.
	if !failed && s.cfg.MaxContextTokens > 0 && sum.LastContext > s.cfg.MaxContextTokens {
		s.log.Info("conversation rotated: the next wake starts a fresh one", "contextTokens", sum.LastContext, "limit", s.cfg.MaxContextTokens)
		sess = ledger.Session{}
	}
	if err := s.led.SaveSession(sess); err != nil {
		s.log.Error("saving session", "err", err)
	}
	s.status.SessionID = sess.ID

	s.checkPermissionMode(sum.GotInit, sum.PermissionMode)

	turn := &Turn{
		At: s.lastTurnEnd.UTC().Format(time.RFC3339), Reason: string(p.reason), Seconds: res.Duration.Seconds(),
		Tokens: u.Billable(), NumTurns: sum.NumTurns, CostUSD: costDelta, Subtype: sum.Subtype,
		IsError: failed, TimedOut: res.TimedOut, Denials: sum.PermissionDenials,
		Terminal: sum.TerminalReason, ExitCode: res.ExitCode, AuthFailed: res.AuthFailed,
		ContextTokens: sum.LastContext,
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
		s.status.LastError = failureText(res, s.cfg.AnthropicAuth, s.cfg.LogContent)
		s.log.Error("turn failed", "err", s.status.LastError, "failures", s.failures, "timedOut", res.TimedOut,
			"tokens", u.Billable())
	} else {
		s.failures, s.status.Failures, s.status.LastError = 0, 0, ""
		s.log.Info("turn done", "tokens", u.Billable(), "numTurns", sum.NumTurns, "seconds", int(res.Duration.Seconds()))
	}

	w := schedule.Consume(s.led.Path(ledger.NextFile), s.cfg.MinWake, s.cfg.MaxWake)
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

	// Baseline after the turn, so the agent's own comments and changes are not
	// edges. Edges that arrived during the turn are folded into this look.
	if issues, err := s.core.ListOpenIssues(ctx); err == nil {
		s.tracker.Rebaseline(issues)
	}

	if !failed {
		s.heartbeat(ctx, w, p.reason)
	} else {
		s.log.Warn("heartbeat not refreshed: the turn failed, so reflection stays the floor")
	}
	s.setState("idle")
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

func (s *Supervisor) sessionLost(res runner.Result) bool {
	if strings.Contains(res.Stderr, "No conversation found") || strings.Contains(strings.Join(res.Summary.Errors, " "), "No conversation found") {
		return true
	}
	return s.failures+1 >= 3 && !res.Summary.GotResult
}

// federationHint is what to read when federation auth fails. The API answers
// every denied exchange with the same opaque 401; the reason is only in the
// Claude Console's authentication history.
const federationHint = "check the authentication history in the Claude Console (jti_reused / match_*)"

func looksLikeAuthFailure(res runner.Result) bool {
	if res.AuthFailed || res.Summary.APIErrorStatus == 401 {
		return true
	}
	text := strings.ToLower(res.Summary.ResultText + " " + strings.Join(res.Summary.Errors, " ") + " " + res.Stderr)
	for _, w := range []string{"authentication failed", "authentication_error", "workloadidentity", "jti_reused", "oauth/token", "token exchange failed with status 401", "token exchange failed with status 403"} {
		if strings.Contains(text, w) {
			return true
		}
	}
	return false
}

// failureText describes why a turn failed. mode is the Anthropic auth mode.
// operationalError reports whether a result text is an error the API or the CLI
// produced, as opposed to text the model wrote.
func operationalError(text string) bool {
	t := strings.ToLower(strings.TrimSpace(text))
	return strings.HasPrefix(t, "api error") || strings.Contains(t, "token exchange failed") ||
		strings.Contains(t, "authentication_error") || strings.Contains(t, "your apikeyhelper script is failing")
}

func failureText(res runner.Result, mode string, content bool) string {
	if mode == config.AuthFederation && looksLikeAuthFailure(res) {
		return "Anthropic authentication failed in federation mode; " + federationHint
	}
	switch {
	case res.AuthFailed:
		return "the Anthropic API rejected the key (401); check the key Secret"
	case res.TimedOut:
		return "turn timed out"
	case len(res.Summary.Errors) > 0:
		return strings.Join(res.Summary.Errors, "; ")
	case res.Summary.IsError && res.Summary.ResultText != "":
		// An error from the API or the CLI is operational and stays; anything
		// else in an error result may be the model's own words.
		if !content && !operationalError(res.Summary.ResultText) {
			return "the turn ended in an error; the model's text is not logged (--log-content shows it)"
		}
		return clip(res.Summary.ResultText, 300)
	case res.Err != nil && res.Stderr != "":
		return res.Err.Error() + ": " + clip(res.Stderr, 300)
	case res.Err != nil:
		return res.Err.Error()
	default:
		return "the CLI wrote no result"
	}
}

// checkPermissionMode reads the init message's permissionMode. If the CLI did
// not start in auto mode, auto is unavailable here (plan, admin setting,
// version): fall back to the configured mode for every later turn.
func (s *Supervisor) checkPermissionMode(gotInit bool, reported string) {
	if !gotInit {
		return
	}
	s.status.PermMode.Reported = reported
	if s.permMode == "auto" && reported != "auto" {
		s.log.Error("the CLI did not start in auto permission mode; falling back",
			"reported", reported, "fallback", s.cfg.FallbackPermMode)
		s.permMode = s.cfg.FallbackPermMode
		s.status.PermMode.InUse, s.status.PermMode.FellBack = s.permMode, true
		if t, ok := s.turner.(interface{ SetPermissionMode(string) }); ok {
			t.SetPermissionMode(s.permMode)
		}
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

func (s *Supervisor) setState(state string) {
	s.status.State = state
	s.status.UpdatedAt = s.now().UTC().Format(time.RFC3339)
	u := s.led.Usage()
	s.status.Tokens = Tokens{Date: u.Date, Used: u.Tokens, Ceiling: s.cfg.DailyTokenCeil, CacheRead: u.CacheRead, CostUSD: u.CostUSD, Turns: u.Turns}
	if err := s.led.WriteStatus(s.status); err != nil {
		s.log.Error("writing status", "err", err)
	}
}
