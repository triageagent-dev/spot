package supervisor

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2aclient"

	"github.com/triageagent-dev/spot/internal/triggers"
)

// The whole task actor over A2A: the core's message is confirmed, the scripted
// turn runs, the note and the bus line are left, and the push the core gets
// ends the task completed with the item's id and the line.
func TestATaskActorOverA2AEndsWithAPushCarryingTheNoteAndTheLine(t *testing.T) {
	const goal = "check the nightly job report"
	var mu sync.Mutex
	var pushes []a2a.Task
	var bearers []string
	core := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == triggers.LinkTasksPath+"/task-9":
			mu.Lock()
			bearers = append(bearers, r.Header.Get("Authorization"))
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"key": "task-9", "goal": goal, "title": "Nightly job", "replyTo": "$req"})
		case r.Method == http.MethodPost && r.URL.Path == triggers.LinkTasksPath+"/task-9/events":
			var task a2a.Task
			_ = json.NewDecoder(r.Body).Decode(&task)
			mu.Lock()
			pushes = append(pushes, task)
			mu.Unlock()
		default:
			t.Errorf("unexpected request %s %s: a task actor has no other call to the core's link", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer core.Close()

	h := newHarness(t, nil)
	sup := taskActor(t, h)
	h.set("summary", "The nightly job passed.")
	ts, err := triggers.NewTaskServer(core.URL, "ws-1", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(ts.Handler())
	defer srv.Close()
	ts.OnStop(sup.StopTask)
	go func() {
		task, err := ts.Wait(context.Background())
		if err != nil {
			return
		}
		res, err := sup.RunTask(context.Background(), task)
		if err != nil {
			res = triggers.TaskResult{Failed: true}
		}
		ts.Finish(res)
	}()

	meta := a2aclient.CallMeta{}
	meta.Append("Authorization", "Bearer tok-9")
	c, err := a2aclient.NewFromEndpoints(context.Background(),
		[]a2a.AgentInterface{{URL: srv.URL, Transport: a2a.TransportProtocolJSONRPC}},
		a2aclient.WithConfig(a2aclient.Config{Polling: true}),
		a2aclient.WithInterceptors(a2aclient.NewStaticCallMetaInjector(meta)))
	if err != nil {
		t.Fatal(err)
	}
	msg := a2a.NewMessage(a2a.MessageRoleUser, a2a.TextPart{Text: goal})
	msg.ContextID = "task-9"
	msg.Metadata = map[string]any{triggers.MetaWorkspace: "ws-1", triggers.MetaTaskKey: "task-9"}
	block := false
	_, err = c.SendMessage(context.Background(), &a2a.MessageSendParams{Message: msg, Config: &a2a.MessageSendConfig{
		Blocking: &block,
		PushConfig: &a2a.PushConfig{
			URL: core.URL + triggers.LinkTasksPath + "/task-9/events", Token: "task-9",
			Auth: &a2a.PushAuthInfo{Schemes: []string{"Bearer"}, Credentials: "tok-9"},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}

	h.waitFor("the task to end", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(pushes) > 0 && pushes[len(pushes)-1].Status.State.Terminal()
	})
	mu.Lock()
	defer mu.Unlock()
	last := pushes[len(pushes)-1]
	if last.Status.State != a2a.TaskStateCompleted || len(last.Artifacts) != 1 || len(last.Artifacts[0].Parts) != 1 {
		t.Fatalf("the last push: %+v", last)
	}
	var data map[string]any
	switch p := last.Artifacts[0].Parts[0].(type) {
	case a2a.DataPart:
		data = p.Data
	case *a2a.DataPart:
		data = p.Data
	}
	if data["memory"] != "item-1" || data["line"] != "The nightly job passed." {
		t.Fatalf("the artifact: %+v", data)
	}
	if len(bearers) != 1 || bearers[0] != "Bearer tok-9" {
		t.Fatalf("the core was asked %v", bearers)
	}
	h.core.mu.Lock()
	defer h.core.mu.Unlock()
	if len(h.core.notes) != 1 || h.core.notes[0].Title != "Nightly job" || len(h.core.bus) != 1 || h.core.bus[0].ReplyTo != "$req" || h.core.bus[0].Memory != "item-1" {
		t.Fatalf("note %+v, bus %+v", h.core.notes, h.core.bus)
	}
}
