// Package triggers is the supervisor's channel to the core: a websocket the
// core holds open and writes to when something changed, so the supervisor reads
// the core only then, instead of on a timer. The frames carry no content (no
// issue text, no message): they say that issues changed or a room has a new
// message, and the supervisor reads the rest with its own permissions. The core
// also tells the spot its name and its token budget over it, and the supervisor
// reports back what each turn cost and how it is doing: counts and labels only.
package triggers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// Path is the core's agent stream.
const Path = "/v2/messenger/spot/ws"

// Budget is the spot's daily token budget as the core holds it. Cap 0 is none;
// Blocked is the core refusing the spot until 00:00 UTC. Spent and Cap are the
// workspace's, shared by all its spots; Own is what this spot spent today as
// the core counted its reported turns.
type Budget struct {
	Spent     int64 `json:"spent"`
	Own       int64 `json:"own"`
	Cap       int64 `json:"cap"`
	Remaining int64 `json:"remaining"`
	Blocked   bool  `json:"blocked"`
}

// Turn is what one turn cost. Input includes the cached part and the cache
// writes.
type Turn struct {
	Model      string `json:"model"`
	Input      int64  `json:"input"`
	Cached     int64  `json:"cached"`
	Output     int64  `json:"output"`
	DurationMs int64  `json:"durationMs,omitempty"`
	Failed     bool   `json:"failed,omitempty"`
}

// Wake is one line of the spot's activity, sent when a wake ends: a turn, or a
// wake that made no model call (quiet, hold, ceiling). Counts and labels, never
// content, so a client can draw a timeline. Next is when the spot wakes again,
// in ms; 0 = not known (a short-lane turn does not set the schedule).
type Wake struct {
	TS         int64  `json:"ts"`
	Reason     string `json:"reason,omitempty"`
	Lane       string `json:"lane,omitempty"`
	Outcome    string `json:"outcome"`
	DurationMs int64  `json:"durationMs,omitempty"`
	Steps      int64  `json:"steps,omitempty"`
	ToolCalls  int64  `json:"toolCalls,omitempty"`
	Billable   int64  `json:"billable,omitempty"`
	CacheRead  int64  `json:"cacheRead,omitempty"`
	Output     int64  `json:"output,omitempty"`
	Next       int64  `json:"next,omitempty"`
	NextSource string `json:"nextSource,omitempty"`
	// NextWhy is the spot's own words for the wake at Next, which the core hands
	// back with the tick. Promise is the wake the agent promised, as it stands
	// when this wake ends; absent = none. A wake of the long or the short lane
	// replaces the core's record of both, so each carries the schedule as it is.
	NextWhy string   `json:"nextWhy,omitempty"`
	Promise *Promise `json:"promise,omitempty"`
	// Trace is one line per model request and tool call of the wake, 40 at most
	// (the first 39, then a "cut" line saying how many were left out): names and
	// numbers, never arguments or results.
	Trace []Step `json:"trace,omitempty"`
	// Key names the wake the way a task list does, so a line of activity and a
	// row of Stats.Tasks are the same wake; Title is a task's (80 bytes at most).
	Key   string `json:"key,omitempty"`
	Title string `json:"title,omitempty"`
}

// Step is one line of a wake's trace. Kind is "model", "tool" or "cut"; Name is
// the model id or the tool's name (64 bytes at most); Tokens are the billable
// tokens of a model request.
type Step struct {
	Kind   string `json:"kind"`
	Name   string `json:"name"`
	Ms     int64  `json:"ms,omitempty"`
	Tokens int64  `json:"tokens,omitempty"`
}

// Promise is the wake the agent promised for later, as the core is told it: At
// in ms; Human says it answers a person's request.
type Promise struct {
	At    int64  `json:"at"`
	Why   string `json:"why,omitempty"`
	Human bool   `json:"human,omitempty"`
}

// Schedule is what the spot has planned, sent once after the core's hello so a
// core that restarted gets it back, and a restarted spot hands over what it kept.
// The core is the spot's scheduler (its ADR-106): it sends the wake when Next or
// the promise is due. BackoffMs is the wait a failed wake left, 0 = none;
// MaxWakeMs is the longest the spot sleeps without a word from the core.
type Schedule struct {
	Next       int64    `json:"next,omitempty"`
	NextWhy    string   `json:"nextWhy,omitempty"`
	NextSource string   `json:"nextSource,omitempty"`
	Promise    *Promise `json:"promise,omitempty"`
	BackoffMs  int64    `json:"backoffMs,omitempty"`
	MaxWakeMs  int64    `json:"maxWakeMs,omitempty"`
}

// Order is a wake the core asks for: Reason is "tick" (the planned wake is due)
// or "promise" (the wake the agent promised is); Why is the spot's own words
// from its last report.
type Order struct {
	Reason string `json:"reason"`
	Why    string `json:"why,omitempty"`
}

// Control is the core asking the spot to act on one of its wakes (a task):
// list, stop, kill, restart or drop. Task is the key a list gave, empty for
// list. The spot answers every Control exactly once, with the same ID.
type Control struct {
	ID   string `json:"id"`
	Op   string `json:"op"`
	Task string `json:"task,omitempty"`
	// Why is the words of a person who asks for a wake (op "wake").
	Why string `json:"why,omitempty"`
}

// TaskLine is one wake the spot has in hand: running, or a task waiting for a
// place. Title is a task's title (80 bytes at most), never its goal.
type TaskLine struct {
	Key       string `json:"key"`
	Title     string `json:"title,omitempty"`
	Lane      string `json:"lane,omitempty"`
	State     string `json:"state"`
	StartedMs int64  `json:"startedMs,omitempty"`
	AgeMs     int64  `json:"ageMs,omitempty"`
}

// ControlResult answers a Control: whether it was done, a short message for the
// person who asked, and what the spot has in hand afterwards.
type ControlResult struct {
	ID    string     `json:"id"`
	OK    bool       `json:"ok"`
	Msg   string     `json:"msg,omitempty"`
	Tasks []TaskLine `json:"tasks,omitempty"`
}

// The longest key and title a task line or an activity line carries.
const (
	MaxTaskKey = 128
	MaxTitle   = 80
)

// Limits of a Control and of its answer.
const (
	maxTaskKey   = MaxTaskKey
	maxTaskLines = 100
	// maxOrderWhy bounds the words a wake order carries.
	maxOrderWhy = 200
	maxTitle    = MaxTitle
	// ControlTimeout is how long a Control may take; the core gives up on its
	// own after 10 s.
	ControlTimeout = 8 * time.Second
)

// ControlFunc acts on a Control. It is given a context that ends at
// ControlTimeout.
type ControlFunc func(context.Context, Control) ControlResult

// Stats is how the spot is doing, a snapshot the core keeps for its clients.
type Stats struct {
	State         string           `json:"state,omitempty"`
	Turns         int64            `json:"turns,omitempty"`
	Failures      int64            `json:"failures,omitempty"`
	Wakes         map[string]int64 `json:"wakes,omitempty"`
	EdgeWakesHour int64            `json:"edgeWakesHour,omitempty"`
	ContextTokens int64            `json:"contextTokens,omitempty"`
	Rotations     int64            `json:"rotations,omitempty"`
	Tokens        int64            `json:"tokens,omitempty"`
	// CacheReadTokens and OutputTokens are today's, beside Tokens (the paid count).
	CacheReadTokens int64   `json:"cacheReadTokens,omitempty"`
	OutputTokens    int64   `json:"outputTokens,omitempty"`
	Ceiling         int64   `json:"ceiling,omitempty"`
	CostUSD         float64 `json:"costUsd,omitempty"`
	NextWake        int64   `json:"nextWake,omitempty"`
	Triggers        string  `json:"triggers,omitempty"`
	Version         string  `json:"version,omitempty"`
	// Pause says why the spot is not waking: "own-ceiling" (its own daily
	// ceiling) or "workspace-cap" (the core's cap); empty when it is not paused.
	Pause string `json:"pause,omitempty"`
	// Config is how the spot is set up, so the core knows its limits.
	Config *Config `json:"config,omitempty"`
	// Tasks is every wake the spot has in hand, running or waiting for a place,
	// at the time of the snapshot; empty when it has none. Each snapshot
	// replaces the last.
	Tasks []TaskLine `json:"tasks,omitempty"`
}

// Config is the spot's limits and choices: no credential, URL or path.
type Config struct {
	Provider       string      `json:"provider,omitempty"`
	Model          string      `json:"model,omitempty"`
	Effort         string      `json:"effort,omitempty"`
	MaxSteps       int         `json:"maxSteps,omitempty"`
	MaxToolCalls   int         `json:"maxToolCalls,omitempty"`
	MaxTurnTokens  int64       `json:"maxTurnTokens,omitempty"`
	MinWake        string      `json:"minWake,omitempty"`
	MaxWake        string      `json:"maxWake,omitempty"`
	ChatBatch      string      `json:"chatBatch,omitempty"`
	EdgeCooldown   string      `json:"edgeCooldown,omitempty"`
	MaxEdgeWakesHr int         `json:"maxEdgeWakesHour,omitempty"`
	CoreTools      string      `json:"coreTools,omitempty"`
	RecallFirst    bool        `json:"recallFirst"`
	PostGate       bool        `json:"postGate"`
	FenceResults   bool        `json:"fenceResults"`
	Connectors     []Connector `json:"connectors,omitempty"`
}

// Connector is one connector the spot may use and the tools it may call there.
type Connector struct {
	Name  string   `json:"name"`
	Tools []string `json:"tools,omitempty"`
}

// Pending is what happened since the last Take.
type Pending struct {
	Hello  bool     // the stream (re)connected: everything may have changed
	Issues bool     // the core's issues or state changed
	Rooms  []string // rooms with a new message
	Name   string   // the core named the spot (again)
	Budget bool     // the core sent the spot's budget (again); read it with Budget()
	// Order is a wake the core asks for; a promise outranks a tick.
	Order *Order
	// Manual is a wake a person asked for from the console; it is kept apart so a
	// tick cannot take its place.
	Manual *Order
}

// Empty reports whether nothing happened.
func (p Pending) Empty() bool {
	return !p.Hello && !p.Issues && len(p.Rooms) == 0 && p.Name == "" && !p.Budget && p.Order == nil && p.Manual == nil
}

// Wake reports whether the core's issues or rooms may have changed, which is
// what makes the supervisor read the core.
func (p Pending) Wake() bool { return p.Hello || p.Issues || len(p.Rooms) > 0 }

// maxOutbox bounds the turns kept while the stream is down.
const maxOutbox = 100

// Client keeps the stream open and remembers what it was told.
type Client struct {
	url       string
	tokenFile string
	log       *slog.Logger

	connected atomic.Bool

	inbox
	outbox []Turn // reported turns the core has not taken yet
	sched  *Schedule

	wmu  sync.Mutex // one writer at a time
	conn *websocket.Conn

	// cert is set when the spot joins by certificate (NewCert): the stream is
	// opened with the join handshake and the token comes over it.
	cert      *Cert
	ready     chan struct{}
	readyOnce sync.Once
	pendingN  int
	// noRenewUntil is set after a failed renewal, so the next connection opens
	// the stream with the certificate it has.
	noRenewUntil time.Time
}

// New builds a client for a core API base URL (http:// or https://) and a
// workspace. The token is read from tokenFile at every connect, so a
// re-minted token is taken without a restart.
func New(apiURL, tenant, tokenFile string, log *slog.Logger) (*Client, error) {
	u, err := url.Parse(strings.TrimRight(apiURL, "/"))
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("core API URL %q is not a URL", apiURL)
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	default:
		return nil, fmt.Errorf("core API URL %q must be http:// or https://", apiURL)
	}
	u.Path = strings.TrimRight(u.Path, "/") + Path
	u.RawQuery = url.Values{"tenant": {tenant}}.Encode()
	return &Client{url: u.String(), tokenFile: tokenFile, log: log, inbox: newInbox()}, nil
}

// Connected reports whether the stream is up. While it is not, the supervisor
// falls back to reading the core on its poll interval.
func (c *Client) Connected() bool { return c.connected.Load() }

// ReportTurn tells the core what a turn cost. A turn the stream cannot take
// now waits for the next connection (the last maxOutbox are kept).
func (c *Client) ReportTurn(t Turn) {
	c.mu.Lock()
	c.outbox = append(c.outbox, t)
	if len(c.outbox) > maxOutbox {
		c.outbox = c.outbox[len(c.outbox)-maxOutbox:]
	}
	c.mu.Unlock()
	c.flush()
}

// SetSchedule sets the schedule the next hello is answered with. The wake
// reports keep the core's copy current while the stream is up.
func (c *Client) SetSchedule(s Schedule) {
	c.mu.Lock()
	c.sched = &s
	c.mu.Unlock()
}

// sendSchedule hands the core the schedule, once per connection.
func (c *Client) sendSchedule() {
	c.mu.Lock()
	s := c.sched
	c.mu.Unlock()
	if s != nil {
		_ = c.send(map[string]any{"schedule": s})
	}
}

// answer runs one control and sends its result, once, whatever happens.
func (c *Client) answer(ctl Control) {
	if ctl.ID == "" || ctl.Op == "" {
		return // nothing to answer to
	}
	c.reply(c.run(ctl))
}

// reply bounds the answer to a control and sends it.
func (c *Client) reply(res ControlResult) {
	if len(res.Tasks) > maxTaskLines {
		res.Tasks = res.Tasks[:maxTaskLines]
	}
	for i := range res.Tasks {
		if t := res.Tasks[i].Title; len(t) > maxTitle {
			res.Tasks[i].Title = strings.ToValidUTF8(t[:maxTitle], "")
		}
	}
	if err := c.send(map[string]any{"controlResult": res}); err != nil {
		c.log.Debug("control not answered: the stream is down", "msg", res.Msg)
	}
}

// askManual keeps a wake a person asked for from the console until the loop takes
// it, and answers the control that carried it. The answer is "asked": the loop
// starts the wake when it is free, or on the short lane at once.
func (c *Client) askManual(id, why string) {
	c.reply(c.manualAnswer(id, why))
}

// ReportWake sends one line of activity. It is dropped while the stream is
// down: a timeline with a gap is the truth, a replay of old wakes is not.
func (c *Client) ReportWake(w Wake) { _ = c.send(map[string]any{"wake": w}) }

// ReportStats sends the supervisor's snapshot; it is dropped while the stream
// is down, the next one replaces it.
func (c *Client) ReportStats(s Stats) { _ = c.send(map[string]any{"stats": s}) }

func (c *Client) send(v any) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if c.conn == nil {
		return errors.New("the stream is down")
	}
	_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return c.conn.WriteJSON(v)
}

// flush sends the waiting turns in order and keeps the ones that fail.
func (c *Client) flush() {
	for {
		c.mu.Lock()
		if len(c.outbox) == 0 {
			c.mu.Unlock()
			return
		}
		t := c.outbox[0]
		c.mu.Unlock()
		if c.send(map[string]any{"turn": t}) != nil {
			return
		}
		c.mu.Lock()
		if len(c.outbox) > 0 {
			c.outbox = c.outbox[1:]
		}
		c.mu.Unlock()
	}
}

type frame struct {
	Hello  bool     `json:"hello"`
	Issues bool     `json:"issues"`
	Rooms  []string `json:"rooms"`
	Name   string   `json:"name"`
	Budget *Budget  `json:"budget"`
	// Wake is the core asking for a wake: the spot's schedule is the core's.
	Wake *Order `json:"wake"`
	// Control is the core asking for an operation on a task.
	Control *Control `json:"control"`
	// Token is the short-lived access token of a certificate identity (the
	// core's ADR-105): bearer for the core's MCP, sent after the hello and again
	// at half its life.
	Token *tokenFrame `json:"token"`
}

const (
	minBackoff = time.Second
	maxBackoff = 30 * time.Second
	// readIdle is how long a silent stream is trusted: the core pings every
	// 30 s.
	readIdle = 90 * time.Second
)

// Run keeps the stream open until ctx ends, reconnecting with a backoff.
func (c *Client) Run(ctx context.Context) {
	backoff := minBackoff
	for ctx.Err() == nil {
		up, err := c.once(ctx)
		c.connected.Store(false)
		if ctx.Err() != nil {
			return
		}
		if up {
			backoff = minBackoff
		}
		c.log.Warn("trigger stream closed; reconnecting", "err", err, "in", backoff.String())
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return
		}
		if backoff *= 2; backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// once opens the stream and reads it until it ends. up reports whether the
// core accepted the connection.
func (c *Client) once(ctx context.Context) (up bool, err error) {
	if c.cert != nil {
		return c.onceCert(ctx)
	}
	tok, err := os.ReadFile(c.tokenFile)
	if err != nil {
		return false, fmt.Errorf("reading the core token: %w", err)
	}
	conn, err := c.dial(ctx, http.Header{"Authorization": {"Bearer " + strings.TrimSpace(string(tok))}})
	if err != nil {
		return false, err
	}
	return c.serve(ctx, conn)
}

// dial opens the websocket. A refusal carries the core's short reason.
func (c *Client) dial(ctx context.Context, hdr http.Header) (*websocket.Conn, error) {
	d := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	conn, resp, err := d.DialContext(ctx, c.url, hdr)
	if err != nil {
		if resp != nil {
			// The core says why in a short JSON error; it holds no content.
			why, _ := io.ReadAll(io.LimitReader(resp.Body, 200))
			return nil, fmt.Errorf("the core refused the stream: %s %s", resp.Status, strings.TrimSpace(string(why)))
		}
		return nil, err
	}
	return conn, nil
}

// serve reads the open stream until it ends, and closes conn. up is true: the
// core accepted the connection.
func (c *Client) serve(ctx context.Context, conn *websocket.Conn) (up bool, err error) {
	defer func() { _ = conn.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	c.wmu.Lock()
	c.conn = conn
	c.wmu.Unlock()
	defer func() {
		c.wmu.Lock()
		c.conn = nil
		c.wmu.Unlock()
	}()
	c.flush()
	_ = conn.SetReadDeadline(time.Now().Add(readIdle))
	conn.SetPingHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(readIdle))
		return conn.WriteControl(websocket.PongMessage, nil, time.Now().Add(5*time.Second))
	})
	for {
		var f frame
		if err := conn.ReadJSON(&f); err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return true, nil
			}
			c.afterClose(err)
			return true, err
		}
		_ = conn.SetReadDeadline(time.Now().Add(readIdle))
		if f.Control != nil && f.Control.Op == "wake" && f.Control.ID != "" {
			go c.askManual(f.Control.ID, f.Control.Why)
			continue
		}
		if f.Control != nil {
			go c.answer(*f.Control)
			continue
		}
		if f.Token != nil {
			c.takeToken(f.Token)
			continue
		}
		if f.Hello {
			c.connected.Store(true)
			c.sendSchedule()
		}
		c.add(f)
	}
}

// clipTo cuts s to n bytes.
func clipTo(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
