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
	"log/slog"
	"os"
	"sort"
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
	// File is the kind of file the message carries ("image/png"), empty when
	// it carries none; read_workspace_file shows it by the message id.
	File     string
	FileName string
	Hop      int // machine-to-machine hop count the core carries; 0 when absent
	// Mentions are the spots the core found named in the message.
	Mentions []string
	// ForYou is the core's answer for this spot: the message names it, or
	// names no spot and this spot is the workspace's oldest or only one. Nil
	// when the core does not say (an older core, or a spot its roster does
	// not know yet): the spot then reads the message as before.
	ForYou *bool
	// ReplyTo is the id of the message this one answers, empty when it is not a
	// reply or the core does not say.
	ReplyTo string
}

// ErrCredentialRejected is what a call returns while the core has refused the
// token in the token file and the file has not changed: nothing is sent until it
// does.
var ErrCredentialRejected = errors.New("the core rejected the credential; waiting for the token file to change")

// Backoff of the connection after a failure: the first retry waits retryMin and
// each failure doubles the wait up to retryMax. A refused credential is not
// retried at all until the token file holds another one.
const (
	retryMin = time.Second
	retryMax = 5 * time.Minute
	// warnEvery is the least time between two warnings about the same trouble.
	warnEvery = time.Minute
)

// Client talks to one core.
type Client struct {
	url       string
	tokenFile string
	tenant    string

	mu    sync.Mutex
	c     *client.Client
	tools map[string]bool

	log *slog.Logger
	now func() time.Time
	// The state of a connection that fails, under mu.
	failures   int       // in a row
	retryAt    time.Time // no attempt before it
	lastErr    error
	denied     bool   // the core refused deniedTok
	deniedTok  string // the token the core refused
	lastWarn   time.Time
	suppressed int
}

// New returns a Client. It connects lazily.
func New(url, tokenFile, tenant string) *Client {
	return &Client{url: url, tokenFile: tokenFile, tenant: tenant, now: time.Now}
}

// SetLogger sets where the client says that the core cannot be reached: one
// warning a minute, whatever the number of calls that failed.
func (c *Client) SetLogger(l *slog.Logger) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.log = l
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

// unauthorized reports whether err is the core refusing the token.
func unauthorized(err error) bool {
	if errors.Is(err, transport.ErrUnauthorized) || errors.Is(err, transport.ErrOAuthAuthorizationRequired) {
		return true
	}
	if err == nil {
		return false
	}
	// The client library wraps a 401 of the initialize call as text.
	m := strings.ToLower(err.Error())
	return strings.Contains(m, "authorization required") || strings.Contains(m, "unauthorized") || strings.Contains(m, "(401)")
}

// open connects unless a failure of the last connection says to wait, and
// holds back every attempt while the core refuses the token in the file. Must be
// called with c.mu held.
func (c *Client) open(ctx context.Context) error {
	if c.c != nil {
		return nil
	}
	tok := c.token()
	if c.denied {
		if tok == c.deniedTok {
			return fmt.Errorf("%w: %v", ErrCredentialRejected, c.lastErr)
		}
		c.denied = false // another token: try it
	}
	if now := c.now(); now.Before(c.retryAt) {
		return fmt.Errorf("the core was not reachable, next try in %s: %w", c.retryAt.Sub(now).Round(time.Second), c.lastErr)
	}
	err := c.connect(ctx)
	if err == nil {
		c.failures, c.lastErr, c.denied = 0, nil, false
		return nil
	}
	c.failed(err, tok)
	return err
}

// failed records a failed connection or call: a refused token stops the
// attempts until the token file changes, any other failure waits longer each
// time, and either is said once a minute.
func (c *Client) failed(err error, tok string) {
	c.lastErr = err
	c.failures++
	if unauthorized(err) {
		c.denied, c.deniedTok = true, tok
	} else {
		d := retryMin << min(c.failures-1, 20)
		if d > retryMax {
			d = retryMax
		}
		c.retryAt = c.now().Add(d)
	}
	if c.log == nil {
		return
	}
	if now := c.now(); now.Sub(c.lastWarn) < warnEvery {
		c.suppressed++
		return
	} else {
		c.lastWarn = now
	}
	next := "when the token file changes"
	if !c.denied {
		next = "in " + c.retryAt.Sub(c.now()).Round(time.Second).String()
	}
	c.log.Warn("the core's MCP endpoint cannot be used", "err", err, "failures", c.failures, "suppressed", c.suppressed, "nextTry", next)
	c.suppressed = 0
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
	ok, _ := c.offers(ctx, name)
	return ok
}

// offers is HasTool with the reason a core that cannot be reached gives: a core
// that is down offers no tool, which is not the same as one that has none.
func (c *Client) offers(ctx context.Context, name string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.open(ctx); err != nil {
		return false, err
	}
	return c.tools[name], nil
}

// call invokes a tool and returns its text. A dropped connection is rebuilt
// once.
func (c *Client) call(ctx context.Context, name string, args map[string]any) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if err := c.open(ctx); err != nil {
			// The attempt that failed has said why, and so has the gate that
			// refused it; a second try in the same call would only repeat it.
			c.reset()
			return "", err
		}
		req := mcp.CallToolRequest{}
		req.Params.Name = name
		req.Params.Arguments = args
		res, err := c.c.CallTool(ctx, req)
		if err != nil {
			lastErr = err
			if unauthorized(err) {
				c.failed(err, c.token())
				c.reset()
				return "", err
			}
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

// MemoryCellContent reads one memory cell by its exact id (get_memory_cell) and
// returns its content. The spot's system prompt is such a cell.
func (c *Client) MemoryCellContent(ctx context.Context, id string) (string, error) {
	text, err := c.call(ctx, "get_memory_cell", map[string]any{"id": id})
	if err != nil {
		return "", err
	}
	var cell struct {
		Content string `json:"content"`
	}
	if err := json.Unmarshal([]byte(text), &cell); err != nil {
		return "", fmt.Errorf("get_memory_cell: unexpected reply: %w", err)
	}
	if strings.TrimSpace(cell.Content) == "" {
		return "", fmt.Errorf("memory cell %q has no content", id)
	}
	return cell.Content, nil
}

// ErrNoRoomTool is returned by RoomMessages when the core does not offer
// read_workspace_room (the core's ADR-095 phase 3 is not deployed yet).
var ErrNoRoomTool = errors.New("core has no read_workspace_room tool")

// RoomMessages reads a workspace room (read_workspace_room) since cursor and
// returns the messages and the cursor to pass next time. The reply shape is
// read loosely because the tool is new: an object with "messages" or a bare
// array; each message with id, sender|from|author, text|body|message and
// ts|time|created_at.
func (c *Client) RoomMessages(ctx context.Context, room, since string) ([]RoomMessage, string, error) {
	ok, err := c.offers(ctx, "read_workspace_room")
	if err != nil {
		return nil, since, err
	}
	if !ok {
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

// ErrNoTypingTool means the core predates set_workspace_typing.
var ErrNoTypingTool = errors.New("the core offers no set_workspace_typing tool")

// Typing shows or clears the spot's "typing…" indicator in a room
// (set_workspace_typing). The core clears it by itself after about 30 seconds.
func (c *Client) Typing(ctx context.Context, room string, on bool) error {
	if !c.HasTool(ctx, "set_workspace_typing") {
		return ErrNoTypingTool
	}
	_, err := c.call(ctx, "set_workspace_typing", map[string]any{"tenant": c.tenant, "room": room, "on": on})
	return err
}

// ErrNoIdentityTool means the core predates get_spot_identity.
var ErrNoIdentityTool = errors.New("the core offers no get_spot_identity tool")

// Identity is who the spot is in its workspace's messenger.
type Identity struct {
	Workspace string `json:"workspace"` // the workspace's id
	Name      string `json:"name"`      // the workspace's display name
	SpotName  string `json:"spotName"`  // the name people see and write to
}

// Identity reads the spot's name in the messenger (get_spot_identity).
// The workspace's owner sets it and can change it, so it is read again now
// and then.
func (c *Client) Identity(ctx context.Context) (Identity, error) {
	if !c.HasTool(ctx, "get_spot_identity") {
		return Identity{}, ErrNoIdentityTool
	}
	text, err := c.call(ctx, "get_spot_identity", map[string]any{"tenant": c.tenant})
	if err != nil {
		return Identity{}, err
	}
	var id Identity
	if err := json.Unmarshal([]byte(text), &id); err != nil {
		return Identity{}, fmt.Errorf("get_spot_identity: unexpected reply: %w", err)
	}
	return id, nil
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
		rm.ReplyTo = str(m, "replyTo", "reply_to")
		if t, ok := m["tria"].(bool); ok {
			rm.Tria = t
		}
		if f, ok := m["file"].(map[string]any); ok {
			rm.File, rm.FileName = str(f, "mime"), str(f, "name")
			if rm.File == "" {
				rm.File = "file"
			}
		}
		if h, ok := m["hop"].(float64); ok {
			rm.Hop = int(h)
		}
		if ms, ok := m["mentions"].([]any); ok {
			for _, v := range ms {
				if n, ok := v.(string); ok && n != "" {
					rm.Mentions = append(rm.Mentions, n)
				}
			}
		}
		if f, ok := m["forYou"].(bool); ok {
			rm.ForYou = &f
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

// Note is a memory item of the spot's own, as the wake shows it.
type Note struct {
	ID      string   `json:"id"`
	Kind    string   `json:"kind"`
	Title   string   `json:"title"`
	Content string   `json:"content"`
	Tags    []string `json:"tags"`
	Updated string   `json:"updated_at"`
}

// StandingNotes reads the spot's own notes (tag spot-note and spot:<name>,
// kinds stable_fact and note, any scope) and the rules people gave it (tag
// spot-rule), by tag and kind, not by a query. It returns them as lines for the
// wake prompt, rules first and then the newest notes, and whether a hold rule
// is active: a person's instruction to check nothing until they ask.
func (c *Client) StandingNotes(ctx context.Context, spot string) (lines []string, hold bool, err error) {
	var all []Note
	for _, kind := range []string{"stable_fact", "note"} {
		text, err := c.call(ctx, "get_memory_items", map[string]any{"tenant": c.tenant, "kind": kind, "status": "active", "limit": 50})
		if err != nil {
			return nil, false, err
		}
		var items []Note
		if err := json.Unmarshal([]byte(text), &items); err != nil {
			var w struct {
				Items []Note `json:"items"`
			}
			if err2 := json.Unmarshal([]byte(text), &w); err2 != nil {
				return nil, false, fmt.Errorf("get_memory_items: unexpected reply: %w", err)
			}
			items = w.Items
		}
		all = append(all, items...)
	}
	lines, hold = SelectNotes(all, spot)
	return lines, hold, nil
}

const (
	maxRuleLines = 8
	maxNoteLines = 4
	noteClip     = 320
)

// SelectNotes picks the standing notes out of memory items: rules first (tag
// spot-rule, or a title that says rule), then the spot's own notes, newest
// first, a few of each. hold is true when a rule tells the spot to check
// nothing until a person asks (tag spot-hold, or wording to that effect).
func SelectNotes(items []Note, spot string) (lines []string, hold bool) {
	var rules, notes []Note
	seen := map[string]bool{}
	for _, it := range items {
		if seen[it.ID] {
			continue
		}
		seen[it.ID] = true
		var own, rule, hd bool
		for _, t := range it.Tags {
			switch t {
			case "spot-rule":
				rule = true
			case "spot-hold":
				rule, hd = true, true
			}
			// The core stamps what the spot writes with its service account; a
			// note the spot rewrote (supersede) keeps that and loses its own tags.
			if t == "spot:"+spot || (strings.HasPrefix(t, "author/") && strings.HasSuffix(t, ":"+spot)) {
				own = true
			}
		}
		if !rule && strings.Contains(strings.ToLower(it.Title), "rule") {
			rule = own || len(it.Tags) == 0 || hasTag(it.Tags, "spot-note")
		}
		switch {
		case rule:
			rules = append(rules, it)
			if hd || holdWording(it.Title+" "+it.Content) {
				hold = true
			}
		case own:
			notes = append(notes, it)
		}
	}
	newest := func(a []Note) {
		sort.SliceStable(a, func(i, j int) bool { return a[i].Updated > a[j].Updated })
	}
	newest(rules)
	newest(notes)
	add := func(prefix string, it Note) {
		line := strings.Join(strings.Fields(it.Title+": "+it.Content), " ")
		if len(line) > noteClip {
			line = strings.ToValidUTF8(line[:noteClip], "") + "…"
		}
		lines = append(lines, prefix+line)
	}
	for i, r := range rules {
		if i == maxRuleLines {
			break
		}
		add("RULE: ", r)
	}
	for i, n := range notes {
		if i == maxNoteLines {
			break
		}
		add("", n)
	}
	return lines, hold
}

func hasTag(tags []string, want string) bool {
	for _, t := range tags {
		if t == want {
			return true
		}
	}
	return false
}

// holdWording reports whether a rule's text says: check nothing until a person
// asks. It is a fallback for a rule the spot wrote without the spot-hold tag.
func holdWording(text string) bool {
	t := strings.ToLower(text)
	negated := false
	for _, w := range []string{"dont", "don't", "do not", "never", "no autonomous", "without a human", "without human", "not check", "stop check"} {
		if strings.Contains(t, w) {
			negated = true
			break
		}
	}
	asks := false
	for _, w := range []string{" ask", " aks", "human request", "requested by a human", "a human asks", "until a person", "before i "} {
		if strings.Contains(t, w) {
			asks = true
			break
		}
	}
	return negated && asks && (strings.Contains(t, "check") || strings.Contains(t, "investigat") || strings.Contains(t, "status"))
}
