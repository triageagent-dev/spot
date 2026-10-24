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
// Blocked is the core refusing the spot until 00:00 UTC.
type Budget struct {
	Spent     int64 `json:"spent"`
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
}

// Pending is what happened since the last Take.
type Pending struct {
	Hello  bool     // the stream (re)connected: everything may have changed
	Issues bool     // the core's issues or state changed
	Rooms  []string // rooms with a new message
	Name   string   // the core named the spot (again)
	Budget bool     // the core sent the spot's budget (again); read it with Budget()
}

// Empty reports whether nothing happened.
func (p Pending) Empty() bool {
	return !p.Hello && !p.Issues && len(p.Rooms) == 0 && p.Name == "" && !p.Budget
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
	signal    chan struct{}

	mu      sync.Mutex
	pending Pending
	budget  *Budget
	outbox  []Turn // reported turns the core has not taken yet

	wmu  sync.Mutex // one writer at a time
	conn *websocket.Conn
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
	return &Client{url: u.String(), tokenFile: tokenFile, log: log, signal: make(chan struct{}, 1)}, nil
}

// Connected reports whether the stream is up. While it is not, the supervisor
// falls back to reading the core on its poll interval.
func (c *Client) Connected() bool { return c.connected.Load() }

// C signals that something is pending; it is coalesced, so one receive may
// stand for many frames.
func (c *Client) C() <-chan struct{} { return c.signal }

// Take returns what happened since the last call and clears it.
func (c *Client) Take() Pending {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.pending
	c.pending = Pending{}
	return p
}

// Budget is the last budget the core sent, false before any.
func (c *Client) Budget() (Budget, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.budget == nil {
		return Budget{}, false
	}
	return *c.budget, true
}

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

func (c *Client) add(f frame) {
	c.mu.Lock()
	if f.Name != "" {
		c.pending.Name = f.Name
	}
	if f.Budget != nil {
		b := *f.Budget
		c.budget = &b
		c.pending.Budget = true
	}
	c.pending.Hello = c.pending.Hello || f.Hello
	c.pending.Issues = c.pending.Issues || f.Issues
	for _, r := range f.Rooms {
		seen := false
		for _, x := range c.pending.Rooms {
			seen = seen || x == r
		}
		if !seen {
			c.pending.Rooms = append(c.pending.Rooms, r)
		}
	}
	c.mu.Unlock()
	select {
	case c.signal <- struct{}{}:
	default:
	}
}

type frame struct {
	Hello  bool     `json:"hello"`
	Issues bool     `json:"issues"`
	Rooms  []string `json:"rooms"`
	Name   string   `json:"name"`
	Budget *Budget  `json:"budget"`
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
	tok, err := os.ReadFile(c.tokenFile)
	if err != nil {
		return false, fmt.Errorf("reading the core token: %w", err)
	}
	d := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	conn, resp, err := d.DialContext(ctx, c.url, http.Header{"Authorization": {"Bearer " + strings.TrimSpace(string(tok))}})
	if err != nil {
		if resp != nil {
			// The core says why in a short JSON error; it holds no content.
			why, _ := io.ReadAll(io.LimitReader(resp.Body, 200))
			return false, fmt.Errorf("the core refused the stream: %s %s", resp.Status, strings.TrimSpace(string(why)))
		}
		return false, err
	}
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
			return true, err
		}
		_ = conn.SetReadDeadline(time.Now().Add(readIdle))
		if f.Hello {
			c.connected.Store(true)
		}
		c.add(f)
	}
}
