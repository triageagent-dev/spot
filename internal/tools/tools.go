// Package tools is the spot's tool surface. The model gets the tools of the
// core's MCP server, of ego when it is configured and of each connector, named
// <server>__<tool>, plus two the spot provides itself (schedule_next and
// finish). The spot has no shell, no filesystem tools and no cluster access of
// its own: a cluster is reached only through a connector.
package tools

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/triageagent-dev/spot/internal/llm"
	"github.com/triageagent-dev/spot/internal/mcptools"
)

// Server is one MCP server the spot takes tools from.
type Server struct {
	// Name is the prefix of its tools in the model's tool list.
	Name string
	URL  string
	// TokenFile holds a bearer token, read on every request; empty = none (a
	// connector holds its own credentials).
	TokenFile string
	// Allow is the exact tool names the model may call. Deny names are
	// refused even when allowed. AllowAll offers every tool the server has
	// except the denied ones.
	Allow    []string
	Deny     []string
	AllowAll bool
}

// Options configure a Registry.
type Options struct {
	// NotesOnly offers the model one tool, the core's search_memory: nothing
	// from a connector or ego, no other core tool, no schedule_next or finish.
	NotesOnly bool
	// RoomSince gives, for a room, the timestamp (ms) of the newest message the
	// last finished turn could see; read_workspace_room is then asked only for
	// what is newer, unless the model names its own since_ts. Nil or 0 = no
	// narrowing.
	RoomSince func(room string) int64
	// Core is the core's MCP server. Without it a turn cannot run.
	Core Server
	// Others are ego and the connectors. One that cannot be reached is left
	// out of the turn.
	Others []Server
	// CallTimeout bounds one tool call; zero = 60 seconds.
	CallTimeout time.Duration
	// ResultMax caps one result in bytes; zero = 32768.
	ResultMax int
	// SpotName is the spot's name. Every memory item the spot writes to the
	// core carries the tag spot:<name>, whether or not the model added it, so
	// the spot finds its own items again. Empty = no tag is added.
	SpotName string
	// RecallFirst holds back every tool but RecallTools (and ego's) in a turn
	// until the model has searched its notes with search_memory, so it never
	// acts on a subject before it has looked for what it was told about it.
	// It does nothing when the core does not offer search_memory.
	RecallFirst bool
	// PostGate holds back the tools that post (PostTools) until the model has
	// read what the core holds about its subject (ContextTools, or a successful
	// search_memory) and written a note about it (NoteTools) in the turn, so it
	// posts knowing what the
	// workspace already has and what it said before. A step is skipped when
	// the core does not offer a tool that could satisfy it.
	PostGate bool
	// FenceResults wraps what a core or connector tool returns in a tag that
	// carries a random id, so text from outside (a log line, an issue comment, a
	// chat message) is marked as data and cannot close the tag to pass for the
	// supervisor's own words. The ego tools and the built-ins are not wrapped.
	FenceResults bool
	Logger       *slog.Logger
}

// Defaults.
const (
	DefaultCallTimeout = 60 * time.Second
	DefaultResultMax   = 32768
	// connectTimeout bounds the connection and listing of one server at the
	// start of a turn, so a slow connector cannot eat the turn.
	connectTimeout = 20 * time.Second
)

// ErrCoreDown means the core's MCP server could not be connected or listed.
var ErrCoreDown = errors.New("the core MCP server is not reachable")

// ScheduleRequest is what the model asked schedule_next for.
type ScheduleRequest struct {
	After string
	Why   string
}

type route struct {
	server string
	tool   string
}

type session struct {
	conn *mcptools.Conn
}

// Registry holds the connections of one turn and the state of the built-in
// tools. Begin starts a turn and End closes its connections; List and Call are
// used between them, by one turn at a time.
type Registry struct {
	o   Options
	log *slog.Logger

	mu       sync.Mutex
	sessions map[string]*session
	routes   map[string]route
	down     []string
	next     *ScheduleRequest
	summary  string
	finished bool
	recalled bool // a search_memory succeeded in this turn
	limited  bool // List offers only HoldTools and the built-ins (see Limit)
	read     bool // a ContextTools call succeeded in this turn
	noted    bool // a NoteTools call succeeded in this turn
}

// New returns a Registry.
func New(o Options) *Registry {
	if o.CallTimeout <= 0 {
		o.CallTimeout = DefaultCallTimeout
	}
	if o.ResultMax <= 0 {
		o.ResultMax = DefaultResultMax
	}
	log := o.Logger
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Registry{o: o, log: log}
}

// Limit narrows the next List to HoldTools and the built-ins, and drops every
// other server (the connectors and ego). The agent sets it before each turn.
func (r *Registry) Limit(on bool) {
	r.mu.Lock()
	r.limited = on
	r.mu.Unlock()
}

// Begin starts a turn: what the built-in tools recorded in the last one is
// forgotten and any connection left open is closed.
func (r *Registry) Begin() {
	r.End()
	r.mu.Lock()
	r.next, r.summary, r.finished, r.recalled, r.read, r.noted = nil, "", false, false, false, false
	r.mu.Unlock()
}

// End closes the turn's connections. What the built-ins recorded stays
// readable until the next Begin.
func (r *Registry) End() {
	r.mu.Lock()
	ss := r.sessions
	r.sessions, r.routes = nil, nil
	r.mu.Unlock()
	for _, s := range ss {
		s.conn.Close()
	}
}

// Schedule is the last valid schedule_next call of the turn, or nil.
func (r *Registry) Schedule() *ScheduleRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.next == nil {
		return nil
	}
	n := *r.next
	return &n
}

// Finished reports whether the model called finish, and with what summary.
func (r *Registry) Finished() (summary string, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.summary, r.finished
}

// Down is the connectors (and ego) that could not be reached when the tools
// were last listed.
func (r *Registry) Down() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.down...)
}

var toolNameOK = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

type listed struct {
	srv   Server
	conn  *mcptools.Conn
	tools []mcptools.Tool
	err   error
}

// List connects to the servers and returns the tools the model may call, each
// named <server>__<tool>, sorted by name, with the server's input schema
// unchanged, and the built-ins. If the core cannot be connected or listed it
// returns an error wrapping ErrCoreDown. A connector that cannot is logged and
// left out; Down names it.
func (r *Registry) List(ctx context.Context) ([]llm.ToolDef, error) {
	r.End()
	servers := append([]Server{r.o.Core}, r.o.Others...)
	r.mu.Lock()
	limited := r.limited
	r.mu.Unlock()
	if r.o.NotesOnly || limited {
		servers = servers[:1]
	}
	out := make([]listed, len(servers))
	var wg sync.WaitGroup
	for i, s := range servers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, connectTimeout)
			defer cancel()
			l := listed{srv: s}
			l.conn, l.err = mcptools.Connect(cctx, s.URL, s.TokenFile, r.o.CallTimeout+5*time.Second)
			if l.err == nil {
				if l.tools, l.err = l.conn.Tools(cctx); l.err != nil {
					l.conn.Close()
					l.conn = nil
				}
			}
			out[i] = l
		}()
	}
	wg.Wait()

	if out[0].err != nil {
		for _, l := range out[1:] {
			if l.conn != nil {
				l.conn.Close()
			}
		}
		return nil, fmt.Errorf("%w: %v", ErrCoreDown, out[0].err)
	}
	sessions := map[string]*session{}
	routes := map[string]route{}
	var defs []llm.ToolDef
	var down []string
	for _, l := range out {
		if l.err != nil {
			r.log.Warn("tool server left out of this turn", "server", l.srv.Name, "err", l.err)
			down = append(down, l.srv.Name)
			continue
		}
		sessions[l.srv.Name] = &session{conn: l.conn}
		for _, t := range l.tools {
			if r.o.NotesOnly && t.Name != searchMemory {
				continue
			}
			if limited && !inList(HoldTools, t.Name) {
				continue
			}
			if !allowed(l.srv, t.Name) {
				continue
			}
			name := l.srv.Name + Sep + t.Name
			if !toolNameOK.MatchString(name) {
				r.log.Warn("tool skipped: its name is not one the model API takes", "server", l.srv.Name, "tool", t.Name)
				continue
			}
			routes[name] = route{server: l.srv.Name, tool: t.Name}
			defs = append(defs, llm.ToolDef{Name: name, Description: t.Description, InputSchema: t.InputSchema})
		}
	}
	if !r.o.NotesOnly {
		defs = append(defs, builtinDefs()...)
	}
	sort.Slice(defs, func(i, j int) bool { return defs[i].Name < defs[j].Name })
	sort.Strings(down)

	r.mu.Lock()
	r.sessions, r.routes, r.down = sessions, routes, down
	r.mu.Unlock()
	return defs, nil
}

// allowed applies the server's lists to an unprefixed tool name.
func allowed(s Server, tool string) bool {
	for _, d := range s.Deny {
		if d == tool {
			return false
		}
	}
	if s.AllowAll {
		return true
	}
	for _, a := range s.Allow {
		if a == tool {
			return true
		}
	}
	return false
}

// Call runs the tool the model named. It never fails the turn: an unknown
// tool, a tool error, a transport error and a timeout all come back as an
// error result the model can read.
func (r *Registry) Call(ctx context.Context, name string, input json.RawMessage) (content string, isError bool) {
	name = r.canonicalName(name)
	if r.o.NotesOnly && IsBuiltin(name) {
		return fmt.Sprintf("unknown tool %q: it was not in your tool list", name), true
	}
	switch name {
	case ScheduleNext:
		return r.scheduleNext(input)
	case Finish:
		return r.finish(input)
	}
	r.mu.Lock()
	rt, ok := r.routes[name]
	s := r.sessions[rt.server]
	held := ok && r.heldForRecall(rt)
	gate := ""
	if ok && !held {
		gate = r.heldForPost(rt)
	}
	r.mu.Unlock()
	if !ok || s == nil {
		return fmt.Sprintf("unknown tool %q: it was not in your tool list", name), true
	}
	if held {
		return recallFirstText, true
	}
	if gate != "" {
		return gate, true
	}
	args := input
	if len(strings.TrimSpace(string(args))) == 0 {
		args = json.RawMessage(`{}`)
	}
	if rt.server == CoreServer && rt.tool == upsertMemoryItem && r.o.SpotName != "" {
		args = withSpotTag(args, SpotTagPrefix+r.o.SpotName)
	}
	if rt.server == CoreServer && rt.tool == readWorkspaceRoom && r.o.RoomSince != nil {
		args = withRoomSince(args, r.o.RoomSince)
	}
	cctx, cancel := context.WithTimeout(ctx, r.o.CallTimeout)
	defer cancel()
	res, err := s.conn.Call(cctx, rt.tool, args)
	switch {
	case err == nil:
		if rt.server == CoreServer && !res.IsError {
			r.mu.Lock()
			switch {
			case rt.tool == searchMemory:
				// The search of the spot's own notes is also the read the post
				// gate wants: what the core holds about the subject, found by
				// its words.
				r.recalled, r.read = true, true
			case inList(ContextTools, rt.tool):
				r.read = true
			case inList(NoteTools, rt.tool):
				r.noted = true
			}
			r.mu.Unlock()
		}
		return r.fence(rt, r.cut(res.Text)), res.IsError
	case ctx.Err() != nil:
		return "the call was cancelled", true
	case errors.Is(cctx.Err(), context.DeadlineExceeded):
		return fmt.Sprintf("the call timed out after %s", r.o.CallTimeout), true
	default:
		return r.cut("the call failed: " + err.Error()), true
	}
}

// fence wraps the text of a core or connector result in a data tag with a
// random id, when Options.FenceResults is set. The id changes with every call
// and the model cannot know it before the result is written, so nothing inside
// the text can close the tag.
func (r *Registry) fence(rt route, text string) string {
	if !r.o.FenceResults || rt.server == EgoServer {
		return text
	}
	id := rand.Text()
	return fmt.Sprintf("<data id=%s from=%s%s%s>\n%s\n</data id=%s>", id, rt.server, Sep, rt.tool, text, id)
}

// recallFirstText is the result of a call held back until the notes are searched.
const recallFirstText = "not run: search your notes first. Call search_memory with `spot-note` and the subject " +
	"you are about to work on (and with what a person told you about it), read what comes back, " +
	"then call this again. A note from a person (skip, never, stop, always) is a rule."

// heldForRecall reports whether a call to rt waits for a search_memory. The
// caller holds r.mu.
func (r *Registry) heldForRecall(rt route) bool {
	if !r.o.RecallFirst || r.recalled {
		return false
	}
	if _, ok := r.routes[CoreServer+Sep+searchMemory]; !ok {
		return false
	}
	if rt.server == EgoServer {
		return false
	}
	if rt.server == CoreServer {
		for _, n := range RecallTools {
			if rt.tool == n {
				return false
			}
		}
	}
	return true
}

const (
	readFirstText = "not run: read what the core holds first. Call get_memory_context (or get_memory_items) for the " +
		"service or subject you are about to post about, see what the workspace already has and what you said before, " +
		"then call this again."
	noteFirstText = "not run: write your note first. Call upsert_memory_item (or supersede_memory_item) with one small " +
		"note about what you are about to say, tagged spot-note, so the next wake knows you said it; search first and " +
		"extend your own note if one exists. Then call this again."
)

// Held reports whether result is a gate holding a call back (the notes were
// not searched, the core's memory not read, or no note written): the call did
// not run, and the model is to do that and call again.
func Held(result string) bool {
	for _, t := range []string{"not run: search your notes first", "not run: read what the core holds first", "not run: write your note first"} {
		if strings.HasPrefix(result, t) {
			return true
		}
	}
	return false
}

// heldForPost returns the refusal text of a call to rt that waits for a memory
// read or a note, or "" when it may run. The caller holds r.mu.
func (r *Registry) heldForPost(rt route) string {
	if !r.o.PostGate || rt.server != CoreServer || !inList(PostTools, rt.tool) {
		return ""
	}
	if !r.read && r.offers(ContextTools) {
		return readFirstText
	}
	if !r.noted && r.offers(NoteTools) {
		return noteFirstText
	}
	return ""
}

// offers reports whether the core offers any of the tools. The caller holds
// r.mu.
func (r *Registry) offers(list []string) bool {
	for _, n := range list {
		if _, ok := r.routes[CoreServer+Sep+n]; ok {
			return true
		}
	}
	return false
}

func inList(list []string, name string) bool {
	for _, n := range list {
		if n == name {
			return true
		}
	}
	return false
}

// IsFinish reports whether name is the spot's finish tool, with or without a
// prefix the model added.
func IsFinish(name string) bool {
	if i := strings.LastIndexAny(name, ":."); i >= 0 && i < len(name)-1 {
		name = name[i+1:]
	}
	return name == Finish
}

// IsBuiltin reports whether name is one of the spot's own tools, schedule_next
// or finish, written with or without a prefix the model added.
func IsBuiltin(name string) bool {
	if i := strings.LastIndexAny(name, ":."); i >= 0 && i < len(name)-1 {
		name = name[i+1:]
	}
	return name == ScheduleNext || name == Finish
}

// canonicalName maps a tool name the model wrote to the one it was offered.
// Some models put their own prefix before the name (Gemini writes
// "default_api:core__list_open_issues", others "functions.finish"). A name that
// is offered is used as it is; otherwise what follows the last ':' or '.' is
// used when that is an offered name. Nothing else is guessed: a name that is
// not offered after stripping stays unknown.
func (r *Registry) canonicalName(name string) string {
	if name == ScheduleNext || name == Finish {
		return name
	}
	r.mu.Lock()
	_, ok := r.routes[name]
	r.mu.Unlock()
	if ok {
		return name
	}
	i := strings.LastIndexAny(name, ":.")
	if i < 0 || i == len(name)-1 {
		return name
	}
	bare := name[i+1:]
	if bare == ScheduleNext || bare == Finish {
		return bare
	}
	r.mu.Lock()
	_, ok = r.routes[bare]
	r.mu.Unlock()
	if ok {
		return bare
	}
	return name
}

const readWorkspaceRoom = "read_workspace_room"

// roomReadLimit is how many messages one read of a room returns when the model
// names no limit and the spot narrowed the read.
const roomReadLimit = 10

// withRoomSince asks for only the messages newer than the last finished turn
// could see, and for at most roomReadLimit of them, unless the model passed its
// own since_ts or limit. Arguments it cannot read are returned as they are.
func withRoomSince(args json.RawMessage, since func(room string) int64) json.RawMessage {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(args, &m); err != nil {
		return args
	}
	if _, ok := m["since_ts"]; ok {
		return args
	}
	room := "team"
	if raw, ok := m["room"]; ok {
		var v string
		if json.Unmarshal(raw, &v) == nil && v != "" {
			room = v
		}
	}
	ts := since(room)
	if ts <= 0 {
		return args
	}
	m["since_ts"], _ = json.Marshal(ts)
	if _, ok := m["limit"]; !ok {
		m["limit"], _ = json.Marshal(roomReadLimit)
	}
	out, err := json.Marshal(m)
	if err != nil {
		return args
	}
	return out
}

// withSpotTag adds tag to the comma-separated tags of a memory item the model
// wants written. Arguments it cannot read are returned as they are: the core
// answers them, and the model reads the answer.
func withSpotTag(args json.RawMessage, tag string) json.RawMessage {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(args, &m); err != nil {
		return args
	}
	var tags string
	if raw, ok := m["tags"]; ok && string(raw) != "null" {
		if err := json.Unmarshal(raw, &tags); err != nil {
			return args
		}
	}
	for _, t := range strings.Split(tags, ",") {
		if strings.TrimSpace(t) == tag {
			return args
		}
	}
	if strings.TrimSpace(tags) != "" {
		tags = strings.TrimRight(strings.TrimSpace(tags), ",") + "," + tag
	} else {
		tags = tag
	}
	m["tags"], _ = json.Marshal(tags)
	out, err := json.Marshal(m)
	if err != nil {
		return args
	}
	return out
}

// cut caps a result at ResultMax bytes, at a character boundary, and says how
// much was dropped.
func (r *Registry) cut(s string) string {
	if len(s) <= r.o.ResultMax {
		return s
	}
	n := r.o.ResultMax
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return fmt.Sprintf("%s\n[result truncated: %d bytes dropped]", s[:n], len(s)-n)
}

func builtinDefs() []llm.ToolDef {
	return []llm.ToolDef{
		{
			Name: ScheduleNext,
			Description: "Say when you want to be woken next. Call it once, as the last thing you do in a turn. " +
				"The supervisor keeps the time between its minimum and maximum wake. Without a call you are woken at the minimum.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{` +
				`"after":{"type":"string","description":"A Go duration such as 90s, 15m or 2h."},` +
				`"why":{"type":"string","description":"A short reason, for yourself: you are told it when you wake."}},` +
				`"required":["after","why"],"additionalProperties":false}`),
		},
		{
			Name: Finish,
			Description: "End this turn. Optional: ending your answer without a tool call ends it too. " +
				"The summary is one line that the status file shows.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{` +
				`"summary":{"type":"string","description":"One line: what this turn did."}},` +
				`"required":["summary"],"additionalProperties":false}`),
		},
	}
}

// maxText bounds the free text a built-in keeps.
const maxText = 400

func (r *Registry) scheduleNext(input json.RawMessage) (string, bool) {
	var in struct {
		After string `json:"after"`
		Why   string `json:"why"`
	}
	if err := json.Unmarshal(input, &in); err != nil {
		return "schedule_next needs a JSON object with after and why: " + err.Error(), true
	}
	d, err := time.ParseDuration(strings.TrimSpace(in.After))
	if err != nil || d <= 0 {
		return fmt.Sprintf("after %q is not a positive duration such as 90s, 15m or 2h; call schedule_next again", in.After), true
	}
	n := ScheduleRequest{After: strings.TrimSpace(in.After), Why: clip(in.Why, maxText)}
	r.mu.Lock()
	r.next = &n
	r.mu.Unlock()
	return "recorded: you will be woken in about " + d.String() + " (the supervisor keeps it within its limits)", false
}

func (r *Registry) finish(input json.RawMessage) (string, bool) {
	var in struct {
		Summary string `json:"summary"`
	}
	if err := json.Unmarshal(input, &in); err != nil {
		return "finish needs a JSON object with summary: " + err.Error(), true
	}
	r.mu.Lock()
	r.summary, r.finished = clip(in.Summary, maxText), true
	r.mu.Unlock()
	return "turn finished", false
}

func clip(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}
