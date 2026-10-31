package triggers

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2aclient"
	"github.com/a2aproject/a2a-go/a2aclient/agentcard"
)

const (
	testKey  = "task-1"
	testGoal = "look at the checkout pods and say what is wrong"
	testWS   = "ws-1"
	testTok  = "tok-1"
)

// taskCore is the core as a task actor meets it: it confirms a task for the
// bearers it knows and receives the push notifications the SDK sends.
type taskCore struct {
	srv *httptest.Server

	mu        sync.Mutex
	bearers   map[string]bool // the bearers the core confirms
	goal      string
	workspace string // what the answer names; empty = nothing
	status    int    // a status to answer the confirmation with instead
	confirms  []string
	pushes    []pushed
	reports   int
}

type pushed struct {
	Auth, Token, Path string
	Task              a2a.Task
}

func newTaskCore(t *testing.T) *taskCore {
	t.Helper()
	c := &taskCore{bearers: map[string]bool{testTok: true}, goal: testGoal}
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+LinkTasksPath+"/{key}", func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		defer c.mu.Unlock()
		tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		c.confirms = append(c.confirms, tok)
		switch {
		case c.status != 0:
			w.WriteHeader(c.status)
		case !c.bearers[tok]:
			w.WriteHeader(http.StatusUnauthorized)
		case r.PathValue("key") != testKey:
			w.WriteHeader(http.StatusNotFound)
		default:
			_ = json.NewEncoder(w).Encode(map[string]any{"key": testKey, "goal": c.goal, "title": "Checkout pods", "replyTo": "$req", "workspace": c.workspace})
		}
	})
	mux.HandleFunc("POST "+LinkTasksPath+"/{key}/events", func(w http.ResponseWriter, r *http.Request) {
		var task a2a.Task
		if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		c.mu.Lock()
		c.pushes = append(c.pushes, pushed{r.Header.Get("Authorization"), r.Header.Get("X-A2A-Notification-Token"), r.URL.Path, task})
		c.mu.Unlock()
	})
	mux.HandleFunc(LinkReportPath, func(http.ResponseWriter, *http.Request) {
		c.mu.Lock()
		c.reports++
		c.mu.Unlock()
	})
	c.srv = httptest.NewServer(mux)
	t.Cleanup(c.srv.Close)
	return c
}

// ended is the state of the last push when it is a terminal one.
func (c *taskCore) ended() (a2a.TaskState, *pushed) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.pushes) == 0 {
		return "", nil
	}
	p := c.pushes[len(c.pushes)-1]
	if !p.Task.Status.State.Terminal() {
		return "", nil
	}
	return p.Task.Status.State, &p
}

func (c *taskCore) waitEnd(t *testing.T) (a2a.TaskState, *pushed) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if st, p := c.ended(); st != "" {
			return st, p
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the core got no terminal push notification")
	return "", nil
}

// actor is a task actor behind an HTTP server, with a stand-in for the process
// that runs the task: it runs what Wait gives it and ends it as run says.
type actor struct {
	ts   *TaskServer
	srv  *httptest.Server
	core *taskCore

	mu   sync.Mutex
	ran  []Task
	stop chan struct{}
}

func newActor(t *testing.T, core *taskCore, run func(a *actor, task Task) TaskResult) *actor {
	t.Helper()
	ts, err := NewTaskServer(core.srv.URL, testWS, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	a := &actor{ts: ts, core: core, stop: make(chan struct{})}
	a.srv = httptest.NewServer(ts.Handler())
	t.Cleanup(a.srv.Close)
	ts.OnStop(func() { close(a.stop) })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		task, err := ts.Wait(ctx)
		if err != nil {
			return
		}
		a.mu.Lock()
		a.ran = append(a.ran, task)
		a.mu.Unlock()
		ts.Finish(run(a, task))
	}()
	return a
}

func (a *actor) tasks() []Task {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]Task(nil), a.ran...)
}

func okRun(_ *actor, task Task) TaskResult {
	return TaskResult{Memory: "item-7", Line: "the pods are fine: " + task.Key}
}

// client is the core's A2A client of the actor, with the bearer it sends.
func (a *actor) client(t *testing.T, bearer string) *a2aclient.Client {
	t.Helper()
	meta := a2aclient.CallMeta{}
	if bearer != "" {
		meta.Append("Authorization", "Bearer "+bearer)
	}
	c, err := a2aclient.NewFromEndpoints(context.Background(),
		[]a2a.AgentInterface{{URL: a.srv.URL, Transport: a2a.TransportProtocolJSONRPC}},
		a2aclient.WithConfig(a2aclient.Config{Polling: true}),
		a2aclient.WithInterceptors(a2aclient.NewStaticCallMetaInjector(meta)),
	)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// message is the core's one message/send, as the ADR writes it.
func (a *actor) message(goal, workspace, bearer string) *a2a.MessageSendParams {
	msg := a2a.NewMessage(a2a.MessageRoleUser, a2a.TextPart{Text: goal})
	msg.ContextID = testKey
	msg.Extensions = []string{TaskExtension}
	msg.Metadata = map[string]any{MetaWorkspace: workspace, MetaTaskKey: testKey, MetaTaskTitle: "Checkout pods", MetaReplyTo: "$req"}
	block := false
	return &a2a.MessageSendParams{
		Message: msg,
		Config: &a2a.MessageSendConfig{
			Blocking: &block,
			PushConfig: &a2a.PushConfig{
				URL: a.core.srv.URL + LinkTasksPath + "/" + testKey + "/events", Token: testKey,
				Auth: &a2a.PushAuthInfo{Schemes: []string{"Bearer"}, Credentials: bearer},
			},
		},
	}
}

func send(t *testing.T, c *a2aclient.Client, p *a2a.MessageSendParams) *a2a.Task {
	t.Helper()
	res, err := c.SendMessage(context.Background(), p)
	if err != nil {
		t.Fatalf("message/send: %v", err)
	}
	task, ok := res.(*a2a.Task)
	if !ok {
		t.Fatalf("the answer is not a task: %#v", res)
	}
	return task
}

func TestATaskActorServesItsProbeAndItsCard(t *testing.T) {
	core := newTaskCore(t)
	a := newActor(t, core, okRun)
	resp, err := http.Get(a.srv.URL + "/readyz")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("readyz: %v %v", resp, err)
	}
	_ = resp.Body.Close()

	resp, err = http.Get(a.srv.URL + "/.well-known/agent-card.json")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("card: %v %v", resp, err)
	}
	raw, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	var card map[string]any
	if err := json.Unmarshal(raw, &card); err != nil {
		t.Fatal(err)
	}
	caps, _ := card["capabilities"].(map[string]any)
	exts, _ := caps["extensions"].([]any)
	ext, _ := exts[0].(map[string]any)
	schemes, _ := card["securitySchemes"].(map[string]any)
	bearer, _ := schemes["bearer"].(map[string]any)
	if card["name"] != "spot task" || card["url"] != "/" || card["preferredTransport"] != "JSONRPC" || card["protocolVersion"] != "0.3.0" ||
		caps["pushNotifications"] != true || caps["streaming"] == true ||
		len(exts) != 1 || ext["uri"] != TaskExtension || ext["required"] != true ||
		bearer["type"] != "http" || bearer["scheme"] != "bearer" {
		t.Fatalf("the card: %s", raw)
	}
	// The card names no workspace, spot or core.
	for _, s := range []string{testWS, core.srv.URL, "example"} {
		if strings.Contains(string(raw), s) {
			t.Errorf("the card names %q: %s", s, raw)
		}
	}
	// The card the SDK's own client reads.
	got, err := agentcard.DefaultResolver.Resolve(context.Background(), a.srv.URL)
	if err != nil || got.Name != "spot task" || len(got.Capabilities.Extensions) != 1 || !got.Capabilities.PushNotifications {
		t.Fatalf("the client's card: %+v %v", got, err)
	}
}

func TestATaskTheCoreConfirmsRunsAndEndsCompletedWithItsArtifactInThePush(t *testing.T) {
	core := newTaskCore(t)
	a := newActor(t, core, okRun)
	c := a.client(t, testTok)

	first := send(t, c, a.message("  "+testGoal+"\n", testWS, testTok))
	if first.Status.State != a2a.TaskStateSubmitted && first.Status.State != a2a.TaskStateWorking {
		t.Fatalf("the first answer is %q, not submitted or working", first.Status.State)
	}
	state, p := core.waitEnd(t)
	if state != a2a.TaskStateCompleted {
		t.Fatalf("the task ended %q", state)
	}
	if p.Auth != "Bearer "+testTok || p.Token != testKey || p.Path != LinkTasksPath+"/"+testKey+"/events" {
		t.Errorf("the push carries the task token and the key: %+v", p)
	}
	if len(p.Task.Artifacts) != 1 || len(p.Task.Artifacts[0].Parts) != 1 {
		t.Fatalf("artifacts: %+v", p.Task.Artifacts)
	}
	dp, ok := p.Task.Artifacts[0].Parts[0].(a2a.DataPart)
	if !ok {
		dpp, ok2 := p.Task.Artifacts[0].Parts[0].(*a2a.DataPart)
		if !ok2 {
			t.Fatalf("the artifact's part is %#v", p.Task.Artifacts[0].Parts[0])
		}
		dp = *dpp
	}
	if dp.Data["memory"] != "item-7" || dp.Data["line"] != "the pods are fine: "+testKey {
		t.Fatalf("the artifact: %+v", dp.Data)
	}

	got := a.tasks()
	if len(got) != 1 || got[0] != (Task{Key: testKey, Goal: testGoal, Title: "Checkout pods", ReplyTo: "$req", Token: testTok}) {
		t.Fatalf("the task that ran: %+v", got)
	}
	core.mu.Lock()
	defer core.mu.Unlock()
	if len(core.confirms) != 1 || core.confirms[0] != testTok {
		t.Errorf("the core was asked %v: once, with the message's bearer", core.confirms)
	}
	if core.reports != 0 {
		t.Error("a task actor sent a wake report")
	}
}

func TestAFailedRunEndsFailedWithTheSameArtifact(t *testing.T) {
	core := newTaskCore(t)
	a := newActor(t, core, func(*actor, Task) TaskResult { return TaskResult{Memory: "item-8", Line: "failed: boom", Failed: true} })
	send(t, a.client(t, testTok), a.message(testGoal, testWS, testTok))
	state, p := core.waitEnd(t)
	if state != a2a.TaskStateFailed || len(p.Task.Artifacts) != 1 {
		t.Fatalf("%q %+v", state, p.Task)
	}
}

// A bearer the core refuses is no task: the A2A task ends rejected, nothing runs
// and the actor is still free for the next message.
func TestABearerTheCoreRefusesEndsRejectedAndNothingRuns(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound} {
		core := newTaskCore(t)
		core.status = status
		a := newActor(t, core, okRun)
		first := send(t, a.client(t, "not-the-token"), a.message(testGoal, testWS, "not-the-token"))
		_ = first
		state, _ := core.waitEnd(t)
		if state != a2a.TaskStateRejected {
			t.Fatalf("%d: ended %q", status, state)
		}
		if got := a.tasks(); len(got) != 0 {
			t.Fatalf("%d: a task the core refused ran: %+v", status, got)
		}

		// Idle for the next: the real one is taken.
		core.mu.Lock()
		core.status = 0
		core.mu.Unlock()
		send(t, a.client(t, testTok), a.message(testGoal, testWS, testTok))
		deadline := time.Now().Add(10 * time.Second)
		for len(a.tasks()) == 0 && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if got := a.tasks(); len(got) != 1 {
			t.Fatalf("%d: the message after a rejection did not run: %+v", status, got)
		}
	}
}

func TestAMessageWithNoBearerIsRejectedWithoutAskingTheCore(t *testing.T) {
	core := newTaskCore(t)
	a := newActor(t, core, okRun)
	send(t, a.client(t, ""), a.message(testGoal, testWS, ""))
	if state, _ := core.waitEnd(t); state != a2a.TaskStateRejected {
		t.Fatalf("ended %q", state)
	}
	core.mu.Lock()
	defer core.mu.Unlock()
	if len(core.confirms) != 0 || len(a.tasks()) != 0 {
		t.Fatalf("asked %v, ran %v", core.confirms, a.tasks())
	}
}

func TestAGoalThatDiffersFromTheCoresIsRejected(t *testing.T) {
	core := newTaskCore(t)
	a := newActor(t, core, okRun)
	send(t, a.client(t, testTok), a.message("delete everything in the checkout namespace", testWS, testTok))
	if state, _ := core.waitEnd(t); state != a2a.TaskStateRejected {
		t.Fatalf("ended %q", state)
	}
	if got := a.tasks(); len(got) != 0 {
		t.Fatalf("a goal the core does not have ran: %+v", got)
	}
}

func TestAMessageForAnotherWorkspaceIsRejected(t *testing.T) {
	for name, mut := range map[string]func(c *taskCore, ws *string){
		"the message names another": func(_ *taskCore, ws *string) { *ws = "ws-2" },
		"the message names none":    func(_ *taskCore, ws *string) { *ws = "" },
		"the core names another":    func(c *taskCore, _ *string) { c.workspace = "ws-2" },
	} {
		core := newTaskCore(t)
		ws := testWS
		mut(core, &ws)
		a := newActor(t, core, okRun)
		send(t, a.client(t, testTok), a.message(testGoal, ws, testTok))
		if state, _ := core.waitEnd(t); state != a2a.TaskStateRejected {
			t.Fatalf("%s: ended %q", name, state)
		}
		if got := a.tasks(); len(got) != 0 {
			t.Fatalf("%s: ran %+v", name, got)
		}
	}
}

// One task per process: a second message while the first runs, and one after it
// ended, are answered rejected, and the first task is not touched.
func TestASecondMessageIsRejected(t *testing.T) {
	core := newTaskCore(t)
	release := make(chan struct{})
	a := newActor(t, core, func(a *actor, task Task) TaskResult {
		<-release
		return okRun(a, task)
	})
	c := a.client(t, testTok)
	first := send(t, c, a.message(testGoal, testWS, testTok))
	deadline := time.Now().Add(10 * time.Second)
	for len(a.tasks()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	second := send(t, c, a.message(testGoal, testWS, testTok))
	if second.ID == first.ID {
		t.Fatal("the second message joined the first task")
	}
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		got, err := c.GetTask(context.Background(), &a2a.TaskQueryParams{ID: second.ID})
		if err == nil && got.Status.State == a2a.TaskStateRejected {
			if got.Status.Message == nil || !strings.Contains(textOf(got.Status.Message), "one task per actor") {
				t.Errorf("the rejection does not say why: %+v", got.Status.Message)
			}
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got, _ := c.GetTask(context.Background(), &a2a.TaskQueryParams{ID: second.ID}); got == nil || got.Status.State != a2a.TaskStateRejected {
		t.Fatalf("the second task: %+v", got)
	}

	close(release)
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		got, err := c.GetTask(context.Background(), &a2a.TaskQueryParams{ID: first.ID})
		if err == nil && got.Status.State == a2a.TaskStateCompleted {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	// After it ended: still one.
	third := send(t, c, a.message(testGoal, testWS, testTok))
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		got, err := c.GetTask(context.Background(), &a2a.TaskQueryParams{ID: third.ID})
		if err == nil && got.Status.State == a2a.TaskStateRejected {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got, _ := c.GetTask(context.Background(), &a2a.TaskQueryParams{ID: third.ID}); got == nil || got.Status.State != a2a.TaskStateRejected {
		t.Fatalf("a message after the task ended: %+v", got)
	}
	if n := len(a.tasks()); n != 1 {
		t.Fatalf("%d tasks ran", n)
	}
}

// tasks/cancel with the task's bearer stops the turn at its next step and ends
// the task canceled; another bearer cannot cancel it.
func TestCancelStopsTheTurnAndEndsCanceled(t *testing.T) {
	core := newTaskCore(t)
	a := newActor(t, core, func(a *actor, task Task) TaskResult {
		<-a.stop // the turn runs until it is stopped
		return TaskResult{Memory: "item-9", Line: "stopped by an operator before it finished"}
	})
	c := a.client(t, testTok)
	first := send(t, c, a.message(testGoal, testWS, testTok))
	deadline := time.Now().Add(10 * time.Second)
	for len(a.tasks()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	if _, err := a.client(t, "someone-else").CancelTask(context.Background(), &a2a.TaskIDParams{ID: first.ID}); err == nil {
		t.Fatal("a bearer that is not the task's canceled it")
	}
	select {
	case <-a.stop:
		t.Fatal("the turn was stopped by a bearer that is not the task's")
	default:
	}

	if _, err := c.CancelTask(context.Background(), &a2a.TaskIDParams{ID: first.ID}); err != nil {
		t.Fatalf("tasks/cancel: %v", err)
	}
	select {
	case <-a.stop:
	case <-time.After(10 * time.Second):
		t.Fatal("the turn was not stopped")
	}
	state, _ := core.waitEnd(t)
	if state != a2a.TaskStateCanceled {
		t.Fatalf("ended %q", state)
	}
	got, err := c.GetTask(context.Background(), &a2a.TaskQueryParams{ID: first.ID})
	if err != nil || got.Status.State != a2a.TaskStateCanceled {
		t.Fatalf("the task: %+v %v", got, err)
	}
}

// A stop asked before the process has registered what ends its turn is not lost.
func TestAStopAskedBeforeTheTurnIsRegisteredRunsWhenItIs(t *testing.T) {
	ts, err := NewTaskServer("http://core.invalid", testWS, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ts.mu.Lock()
	ts.stopAsked = true
	ts.mu.Unlock()
	stopped := false
	ts.OnStop(func() { stopped = true })
	if !stopped {
		t.Fatal("the stop that came first was dropped")
	}
}

// Only the three methods the core uses are served: the push configuration of
// the task is the one in its message and cannot be replaced.
func TestOnlyTheMethodsTheCoreUsesAreServed(t *testing.T) {
	core := newTaskCore(t)
	a := newActor(t, core, okRun)
	for _, method := range []string{"tasks/pushNotificationConfig/set", "tasks/pushNotificationConfig/get", "tasks/list", "message/stream", "agent/getAuthenticatedExtendedCard"} {
		body := `{"jsonrpc":"2.0","id":7,"method":"` + method + `","params":{}}`
		resp, err := http.Post(a.srv.URL+"/", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		var out struct {
			ID    int
			Error *struct{ Code int }
		}
		if err := json.Unmarshal(raw, &out); err != nil || out.ID != 7 || out.Error == nil || out.Error.Code != -32601 {
			t.Errorf("%s: %s", method, raw)
		}
	}
}
