// Package triggers is the supervisor's push channel from the core: a
// websocket the core holds open and writes to when something changed, so the
// supervisor reads the core only then, instead of on a timer. The frames carry
// no content (no issue text, no message): they say that issues changed or a
// room has a new message, and the supervisor reads the rest with its own
// permissions.
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

// Pending is what happened since the last Take.
type Pending struct {
	Hello  bool     // the stream (re)connected: everything may have changed
	Issues bool     // the core's issues or state changed
	Rooms  []string // rooms with a new message
}

// Empty reports whether nothing happened.
func (p Pending) Empty() bool { return !p.Hello && !p.Issues && len(p.Rooms) == 0 }

// Client keeps the stream open and remembers what it was told.
type Client struct {
	url       string
	tokenFile string
	log       *slog.Logger

	connected atomic.Bool
	signal    chan struct{}

	mu      sync.Mutex
	pending Pending
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

func (c *Client) add(f frame) {
	c.mu.Lock()
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
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
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
