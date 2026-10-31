package triggers

// A task actor (the core's ADR-109) is an A2A agent (Agent2Agent protocol 0.3)
// that the core calls as its client. The core makes an actor for one piece of
// work, waits for its probe and sends it one message; the actor never joins,
// never says hello and holds no schedule:
//
//	core → task   GET  /readyz                          the actor's wake-up probe
//	core → task   GET  /.well-known/agent-card.json     the static card (one required extension)
//	core → task   POST /   message/send                 bearer: the task's token
//	task → core   GET  /v2/messenger/spot/link/tasks/{key}   confirms the task (same bearer)
//	task → core   POST <pushNotificationConfig.url>     the SDK's push sender, at every state
//	core → task   POST /   tasks/cancel                 stop and kill
//
// The message carries the goal, its task key and its workspace, but the truth
// stays the core's: the actor asks the core for the task with the message's
// bearer, runs the goal the core answers, and refuses a message whose goal or
// workspace differs. The bearer is the task's access token: the bearer of the
// task's own calls to the core (the tools), nothing else. The core refuses it
// on hello, stats, schedule and control answers, and on any other workspace.
//
// The end of the task is told by the push notification the SDK's sender posts
// to the configuration the core put in its message; the actor sends no wake
// report.

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2asrv"
	"github.com/a2aproject/a2a-go/a2asrv/eventqueue"
	"github.com/a2aproject/a2a-go/a2asrv/push"
)

// TaskExtension is the A2A extension a task actor requires of its caller.
const TaskExtension = "https://triageagent.dev/a2a/spot-task/v1"

// The keys of message.metadata the core sets.
const (
	MetaWorkspace = "triage.workspace"
	MetaTaskKey   = "triage.task.key"
	MetaTaskTitle = "triage.task.title"
	MetaReplyTo   = "triage.task.replyTo"
)

// Task is the piece of work a task actor runs: what the core answered when the
// actor confirmed the message's task, and the bearer that came with it.
type Task struct {
	Key     string
	Goal    string
	Title   string
	ReplyTo string
	Token   string
}

// TaskResult is how a task ended, for the A2A task's final state and artifact.
type TaskResult struct {
	Memory string // the id of the memory item that holds the result; empty when it could not be saved
	Line   string // the line the task left in the bus
	Failed bool   // the run failed
}

// maxTaskGoal bounds the goal the core may send.
const maxTaskGoal = 8000

// confirmTries is how often the actor asks the core to confirm a task when the
// core cannot be reached or answers with a server error.
const confirmTries = 3

// taskCard is the actor's card: the image's own, static. It names no workspace,
// no spot and no core.
func taskCard() *a2a.AgentCard {
	return &a2a.AgentCard{
		Name:               "spot task",
		Description:        "Runs one piece of work the core hands it, leaves the result in memory and in the bus, and ends.",
		URL:                "/",
		PreferredTransport: a2a.TransportProtocolJSONRPC,
		ProtocolVersion:    "0.3.0",
		Version:            "1",
		Capabilities: a2a.AgentCapabilities{
			PushNotifications: true,
			Streaming:         false,
			Extensions:        []a2a.AgentExtension{{URI: TaskExtension, Required: true, Description: "One task per actor, confirmed with the core before it runs."}},
		},
		SecuritySchemes:    a2a.NamedSecuritySchemes{"bearer": a2a.HTTPAuthSecurityScheme{Scheme: "bearer", Description: "The task token the core made for this task."}},
		Security:           []a2a.SecurityRequirements{{"bearer": a2a.SecuritySchemeScopes{}}},
		DefaultInputModes:  []string{"text/plain"},
		DefaultOutputModes: []string{"application/json"},
		Skills:             []a2a.AgentSkill{},
	}
}

// TaskServer is the A2A side of a task actor.
type TaskServer struct {
	base      string
	workspace string
	log       *slog.Logger
	hc        *http.Client

	tasks chan Task       // the confirmed task, for the process to run
	done  chan TaskResult // how it ended, for the executor to say

	mu        sync.Mutex
	claimed   bool
	taskID    a2a.TaskID
	token     string
	stop      func()
	stopAsked bool
}

// NewTaskServer builds the task actor's server for the core's API base URL and
// the workspace the actor was made for (its own, from its settings).
func NewTaskServer(apiURL, workspace string, log *slog.Logger) (*TaskServer, error) {
	u, err := url.Parse(strings.TrimRight(apiURL, "/"))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("core API URL %q must be http:// or https://", apiURL)
	}
	return &TaskServer{
		base: u.String(), workspace: workspace, log: log,
		hc:    &http.Client{Timeout: 30 * time.Second},
		tasks: make(chan Task, 1), done: make(chan TaskResult, 1),
	}, nil
}

// Handler serves the probe, the card and the JSON-RPC endpoint.
func (t *TaskServer) Handler() http.Handler {
	h := a2asrv.NewHandler(&taskExecutor{s: t},
		a2asrv.WithPushNotifications(push.NewInMemoryStore(), push.NewHTTPPushSender(&push.HTTPSenderConfig{Timeout: 30 * time.Second})),
	)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.Handle("GET "+a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(taskCard()))
	mux.Handle("/", onlyTaskMethods(a2asrv.NewJSONRPCHandler(h)))
	return mux
}

// onlyTaskMethods lets through the three methods the core uses: the message, a
// look at the task and its cancelation. The push configuration methods are
// refused: the one configuration the actor pushes to is the one in the message.
func onlyTaskMethods(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			next.ServeHTTP(w, r)
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxFrameBody))
		if err != nil {
			http.Error(w, "the request is too large", http.StatusRequestEntityTooLarge)
			return
		}
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.Unmarshal(body, &req)
		switch req.Method {
		case "message/send", "tasks/get", "tasks/cancel":
			r.Body = io.NopCloser(strings.NewReader(string(body)))
			next.ServeHTTP(w, r)
		default:
			id := req.ID
			if len(id) == 0 {
				id = json.RawMessage("null")
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"error":{"code":-32601,"message":"a task actor takes message/send, tasks/get and tasks/cancel"}}`+"\n", id)
		}
	})
}

// Wait blocks until a task has been confirmed with the core, or ctx ends.
func (t *TaskServer) Wait(ctx context.Context) (Task, error) {
	select {
	case task := <-t.tasks:
		return task, nil
	case <-ctx.Done():
		return Task{}, ctx.Err()
	}
}

// Finish says how the task ended: the executor ends the A2A task with it, and
// the SDK's push sender tells the core. It does not block.
func (t *TaskServer) Finish(r TaskResult) {
	select {
	case t.done <- r:
	default:
	}
}

// OnStop registers what ends the running turn at its next step. A stop asked
// before it is registered runs it at once.
func (t *TaskServer) OnStop(f func()) {
	t.mu.Lock()
	t.stop = f
	asked := t.stopAsked
	t.mu.Unlock()
	if asked {
		f()
	}
}

// taskExecutor is the A2A executor of a task actor.
type taskExecutor struct{ s *TaskServer }

var _ a2asrv.AgentExecutor = (*taskExecutor)(nil)

// bearerFrom is the bearer of the request the SDK is serving.
func bearerFrom(ctx context.Context) string {
	cc, ok := a2asrv.CallContextFrom(ctx)
	if !ok {
		return ""
	}
	vals, _ := cc.RequestMeta().Get("Authorization")
	for _, v := range vals {
		if len(v) > 7 && strings.EqualFold(v[:7], "bearer ") {
			if tok := strings.TrimSpace(v[7:]); tok != "" {
				return tok
			}
		}
	}
	return ""
}

func metaString(m map[string]any, k string) string {
	s, _ := m[k].(string)
	return s
}

// textOf is the text of a message, its text parts joined.
func textOf(m *a2a.Message) string {
	var parts []string
	for _, p := range m.Parts {
		switch tp := p.(type) {
		case a2a.TextPart:
			parts = append(parts, tp.Text)
		case *a2a.TextPart:
			parts = append(parts, tp.Text)
		}
	}
	return strings.Join(parts, "\n")
}

// end writes a final state, with the reason as the status message.
func end(ctx context.Context, rc *a2asrv.RequestContext, q eventqueue.Queue, state a2a.TaskState, why string) error {
	var msg *a2a.Message
	if why != "" {
		msg = a2a.NewMessageForTask(a2a.MessageRoleAgent, rc, a2a.TextPart{Text: why})
	}
	ev := a2a.NewStatusUpdateEvent(rc, state, msg)
	ev.Final = true
	return q.Write(ctx, ev)
}

// Execute takes the message: confirm the task with the core, run it once, say
// how it ended. A message the core does not confirm, or one that is not the
// task's, ends the A2A task rejected and the actor stays idle for the next.
func (e *taskExecutor) Execute(ctx context.Context, rc *a2asrv.RequestContext, q eventqueue.Queue) error {
	s := e.s
	if err := q.Write(ctx, a2a.NewStatusUpdateEvent(rc, a2a.TaskStateSubmitted, nil)); err != nil {
		return err
	}
	reject := func(why string) error {
		s.log.Warn("task message rejected", "why", why)
		return end(ctx, rc, q, a2a.TaskStateRejected, why)
	}
	msg := rc.Message
	bearer := bearerFrom(ctx)
	if msg == nil || bearer == "" {
		return reject("a task message needs a bearer token and a message")
	}
	key := metaString(msg.Metadata, MetaTaskKey)
	if key == "" {
		key = msg.ContextID
	}
	if key == "" {
		return reject("the message names no task")
	}
	info, status, err := s.confirm(ctx, key, bearer)
	switch {
	case ctx.Err() != nil:
		return nil // canceled while it asked
	case status == http.StatusUnauthorized || status == http.StatusForbidden || status == http.StatusNotFound:
		return reject("the core does not confirm this task")
	case err != nil:
		return reject("the core could not confirm the task: " + err.Error())
	}
	goal := strings.TrimSpace(info.Goal)
	switch {
	case info.Key != "" && info.Key != key:
		return reject("the core's task is not the message's")
	case goal == "" || len(goal) > maxTaskGoal:
		return reject("the core's task has no goal, or one over 8000 bytes")
	case strings.TrimSpace(textOf(msg)) != goal:
		return reject("the message's goal is not the one the core has for this task")
	}
	// Protocol 0.3 has no tenant field: the workspace rides in the metadata, and
	// the actor's own is the one it was made for.
	want := s.workspace
	if want == "" {
		want = info.Workspace
	}
	if (want != "" && metaString(msg.Metadata, MetaWorkspace) != want) || (info.Workspace != "" && info.Workspace != want) {
		return reject("the message is for another workspace")
	}
	title := info.Title
	if title == "" {
		title = metaString(msg.Metadata, MetaTaskTitle)
	}
	replyTo := info.ReplyTo
	if replyTo == "" {
		replyTo = metaString(msg.Metadata, MetaReplyTo)
	}

	s.mu.Lock()
	if s.claimed {
		s.mu.Unlock()
		return reject("this actor has its task: one task per actor")
	}
	s.claimed, s.taskID, s.token = true, rc.TaskID, bearer
	s.mu.Unlock()
	if err := q.Write(ctx, a2a.NewStatusUpdateEvent(rc, a2a.TaskStateWorking, nil)); err != nil {
		return err
	}
	s.log.Info("task actor: the task is confirmed", "key", key)
	s.tasks <- Task{Key: key, Goal: goal, Title: title, ReplyTo: replyTo, Token: bearer}

	select {
	case res := <-s.done:
		art := a2a.NewArtifactEvent(rc, a2a.DataPart{Data: map[string]any{"memory": res.Memory, "line": res.Line}})
		art.LastChunk = true
		if err := q.Write(ctx, art); err != nil {
			return err
		}
		if res.Failed {
			return end(ctx, rc, q, a2a.TaskStateFailed, "the task failed")
		}
		return end(ctx, rc, q, a2a.TaskStateCompleted, "")
	case <-ctx.Done():
		return nil // canceled: Cancel wrote the final state
	}
}

// Cancel ends the running turn at its next step and the A2A task canceled. Only
// the bearer the task came with may cancel it.
func (e *taskExecutor) Cancel(ctx context.Context, rc *a2asrv.RequestContext, q eventqueue.Queue) error {
	s := e.s
	s.mu.Lock()
	mine := s.claimed && s.taskID == rc.TaskID
	ok := subtle.ConstantTimeCompare([]byte(s.token), []byte(bearerFrom(ctx))) == 1
	var stop func()
	if mine && ok {
		s.stopAsked = true
		stop = s.stop
	}
	s.mu.Unlock()
	if mine && !ok {
		return a2a.ErrUnauthenticated
	}
	if stop != nil {
		stop()
	}
	return end(ctx, rc, q, a2a.TaskStateCanceled, "")
}

// taskInfo is what the core answers for a task key.
type taskInfo struct {
	Key       string `json:"key"`
	Goal      string `json:"goal"`
	Title     string `json:"title"`
	ReplyTo   string `json:"replyTo"`
	Workspace string `json:"workspace"`
}

// confirm asks the core for the task with the message's bearer. The status is
// the core's last answer (0 when it could not be reached); an error with a
// 401, 403 or 404 is the core refusing, anything else a core that could not say.
func (t *TaskServer) confirm(ctx context.Context, key, bearer string) (taskInfo, int, error) {
	var last error
	var status int
	for i := range confirmTries {
		if i > 0 {
			select {
			case <-time.After(time.Duration(i) * time.Second):
			case <-ctx.Done():
				return taskInfo{}, 0, ctx.Err()
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.base+LinkTasksPath+"/"+url.PathEscape(key), nil)
		if err != nil {
			return taskInfo{}, 0, err
		}
		req.Header.Set("Authorization", "Bearer "+bearer)
		resp, err := t.hc.Do(req)
		if err != nil {
			last, status = err, 0
			continue
		}
		b, _ := io.ReadAll(io.LimitReader(resp.Body, maxFrameBody))
		_ = resp.Body.Close()
		status = resp.StatusCode
		switch {
		case status/100 == 2:
			var info taskInfo
			if err := json.Unmarshal(b, &info); err != nil {
				return taskInfo{}, status, errors.New("the core's answer is not a task")
			}
			return info, status, nil
		case status == http.StatusUnauthorized || status == http.StatusForbidden || status == http.StatusNotFound:
			return taskInfo{}, status, fmt.Errorf("the core answered %s", resp.Status)
		}
		last = fmt.Errorf("the core answered %s", resp.Status)
	}
	return taskInfo{}, status, last
}
