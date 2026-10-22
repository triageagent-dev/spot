// Package corecli is the supervisor's own MCP client for the core: the cheap,
// model-free poll between turns and the heartbeat write. It speaks MCP
// streamable HTTP (github.com/mark3labs/mcp-go, the library the core serves
// with) and reads the bearer token from the token file on every request, so
// a rotated Secret is picked up without a restart.
package corecli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/client/transport"
	"github.com/mark3labs/mcp-go/mcp"
)

// Issue is the subset of list_open_issues the edge detector uses.
type Issue struct {
	ID          string    `json:"id"`
	Severity    string    `json:"severity"`
	Status      string    `json:"status"`
	Source      string    `json:"source"`
	Summary     string    `json:"summary"`
	Tenant      string    `json:"tenant"`
	LastSeen    time.Time `json:"lastSeen"`
	ReopenCount int       `json:"reopenCount"`
	Comments    int       `json:"comments"`
}

// RoomMessage is one message from read_workspace_room.
type RoomMessage struct {
	ID     string
	Sender string // the Matrix user id of the sender
	Name   string // the sender's display name
	Text   string
	Time   string
	TS     int64  // milliseconds; the core's read_workspace_room cursor
	Tria   bool   // Tria wrote it
	Spot   string // a teammate agent wrote it; its name
}

// Client talks to one core.
type Client struct {
	url       string
	tokenFile string
	tenant    string

	mu    sync.Mutex
	c     *client.Client
	tools map[string]bool
}

// New returns a Client. It connects lazily.
func New(url, tokenFile, tenant string) *Client {
	return &Client{url: url, tokenFile: tokenFile, tenant: tenant}
}

func (c *Client) token() string {
	b, err := os.ReadFile(c.tokenFile)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// connect must be called with c.mu held.
func (c *Client) connect(ctx context.Context) error {
	if c.c != nil {
		return nil
	}
	cl, err := client.NewStreamableHttpClient(c.url,
		transport.WithHTTPHeaderFunc(func(context.Context) map[string]string {
			return map[string]string{"Authorization": "Bearer " + c.token()}
		}),
		transport.WithHTTPTimeout(30*time.Second),
	)
	if err != nil {
		return err
	}
	if err := cl.Start(ctx); err != nil {
		return err
	}
	req := mcp.InitializeRequest{}
	req.Params.ProtocolVersion = mcp.LATEST_PROTOCOL_VERSION
	req.Params.ClientInfo = mcp.Implementation{Name: "tria-spot-supervisor", Version: "1"}
	if _, err := cl.Initialize(ctx, req); err != nil {
		_ = cl.Close()
		return fmt.Errorf("initialize: %w", err)
	}
	tools, err := cl.ListTools(ctx, mcp.ListToolsRequest{})
	if err != nil {
		_ = cl.Close()
		return fmt.Errorf("tools/list: %w", err)
	}
	c.tools = map[string]bool{}
	for _, t := range tools.Tools {
		c.tools[t.Name] = true
	}
	c.c = cl
	return nil
}

func (c *Client) reset() {
	if c.c != nil {
		_ = c.c.Close()
	}
	c.c, c.tools = nil, nil
}

// Close drops the connection.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reset()
}

// HasTool reports whether the core offers a tool, as of the last connection.
func (c *Client) HasTool(ctx context.Context, name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.connect(ctx); err != nil {
		return false
	}
	return c.tools[name]
}

// call invokes a tool and returns its text. A dropped connection is rebuilt
// once.
func (c *Client) call(ctx context.Context, name string, args map[string]any) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if err := c.connect(ctx); err != nil {
			lastErr = err
			c.reset()
			continue
		}
		req := mcp.CallToolRequest{}
		req.Params.Name = name
		req.Params.Arguments = args
		res, err := c.c.CallTool(ctx, req)
		if err != nil {
			lastErr = err
			c.reset()
			continue
		}
		text := resultText(res)
		if res.IsError {
			return "", fmt.Errorf("%s: %s", name, text)
		}
		return text, nil
	}
	return "", lastErr
}

func resultText(res *mcp.CallToolResult) string {
	var parts []string
	for _, ct := range res.Content {
		if t, ok := ct.(mcp.TextContent); ok {
			parts = append(parts, t.Text)
		}
	}
	return strings.Join(parts, "\n")
}

// ListOpenIssues returns the tenant's open issues (list_open_issues).
func (c *Client) ListOpenIssues(ctx context.Context) ([]Issue, error) {
	text, err := c.call(ctx, "list_open_issues", map[string]any{"tenant": c.tenant, "limit": 200})
	if err != nil {
		return nil, err
	}
	var r struct {
		Issues []Issue `json:"issues"`
	}
	if err := json.Unmarshal([]byte(text), &r); err != nil {
		return nil, fmt.Errorf("list_open_issues: unexpected reply: %w", err)
	}
	return r.Issues, nil
}

// Heartbeat refreshes the System-2 heartbeat cell (docs/todo/system2-loop-
// contract.md): upsert_memory_item with kind note, a stale_after of roughly
// twice the cadence, so reflection resumes when this stops.
func (c *Client) Heartbeat(ctx context.Context, id, title, content string, staleAfter time.Duration) error {
	_, err := c.call(ctx, "upsert_memory_item", map[string]any{
		"id":          id,
		"kind":        "note",
		"scope":       "global",
		"title":       title,
		"content":     content,
		"tags":        "system2,heartbeat",
		"stale_after": staleAfter.Round(time.Second).String(),
		"tenant":      c.tenant,
	})
	return err
}

// ErrNoRoomTool is returned by RoomMessages when the core does not offer
// read_workspace_room (ADR-095 phase 3 is not deployed yet).
var ErrNoRoomTool = errors.New("core has no read_workspace_room tool")

// RoomMessages reads a workspace room (read_workspace_room) since cursor and
// returns the messages and the cursor to pass next time. The reply shape is
// read loosely because the tool is new: an object with "messages" or a bare
// array; each message with id, sender|from|author, text|body|message and
// ts|time|created_at.
func (c *Client) RoomMessages(ctx context.Context, room, since string) ([]RoomMessage, string, error) {
	if !c.HasTool(ctx, "read_workspace_room") {
		return nil, since, ErrNoRoomTool
	}
	args := map[string]any{"tenant": c.tenant, "room": room}
	// The core's cursor is the ts of the newest message seen, in milliseconds.
	if ts, err := strconv.ParseInt(since, 10, 64); err == nil && ts > 0 {
		args["since_ts"] = ts
	}
	text, err := c.call(ctx, "read_workspace_room", args)
	if err != nil {
		return nil, since, err
	}
	return ParseRoom(text, since)
}

// ParseRoom decodes a read_workspace_room reply. Exposed for tests.
func ParseRoom(text, since string) ([]RoomMessage, string, error) {
	var raw []map[string]any
	var obj struct {
		Messages []map[string]any `json:"messages"`
		Next     string           `json:"next"`
		Cursor   string           `json:"cursor"`
	}
	trim := strings.TrimSpace(text)
	switch {
	case strings.HasPrefix(trim, "["):
		if err := json.Unmarshal([]byte(trim), &raw); err != nil {
			return nil, since, err
		}
	default:
		if err := json.Unmarshal([]byte(trim), &obj); err != nil {
			return nil, since, fmt.Errorf("read_workspace_room: unexpected reply: %w", err)
		}
		raw = obj.Messages
	}
	var out []RoomMessage
	var maxTS int64
	for _, m := range raw {
		rm := RoomMessage{
			ID:     str(m, "id", "event_id"),
			Sender: str(m, "sender", "from", "author"),
			Name:   str(m, "name"),
			Text:   str(m, "text", "body", "message"),
			Time:   str(m, "time", "created_at"),
			Spot:   str(m, "spot"),
		}
		if t, ok := m["tria"].(bool); ok {
			rm.Tria = t
		}
		// ts is a JSON number in milliseconds; fmt.Sprint of a float64 would
		// give scientific notation and lose the cursor.
		if f, ok := m["ts"].(float64); ok {
			rm.TS = int64(f)
			rm.Time = strconv.FormatInt(rm.TS, 10)
		}
		if rm.TS > maxTS {
			maxTS = rm.TS
		}
		out = append(out, rm)
	}
	next := since
	switch {
	case obj.Next != "":
		next = obj.Next
	case obj.Cursor != "":
		next = obj.Cursor
	case maxTS > 0:
		next = strconv.FormatInt(maxTS, 10)
	case len(out) > 0:
		last := out[len(out)-1]
		if last.ID != "" {
			next = last.ID
		} else if last.Time != "" {
			next = last.Time
		}
	}
	return out, next, nil
}

func str(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			if s, ok := v.(string); ok {
				return s
			}
			if v != nil {
				return fmt.Sprint(v)
			}
		}
	}
	return ""
}
