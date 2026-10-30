package triggers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func taskServer(t *testing.T, core string, token string) (*TaskServer, string) {
	t.Helper()
	dir := t.TempDir()
	vf := filepath.Join(dir, "verifier")
	if token != "" {
		sum := sha256.Sum256([]byte(token))
		if err := os.WriteFile(vf, []byte(hex.EncodeToString(sum[:])+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	ts, err := NewTaskServer(core, vf, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return ts, vf
}

func postFrame(t *testing.T, h http.Handler, bearer, body string) int {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/frame", strings.NewReader(body))
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code
}

const taskBody = `{"task":{"key":"task-1","goal":"look at the checkout pods","replyTo":"$req","token":"tok-1"}}`

func TestATaskActorTakesOnlyTheRequestWhoseBearerHashesToItsVerifier(t *testing.T) {
	ts, vf := taskServer(t, "http://core.invalid", "tok-1")
	h := ts.Handler()
	for name, c := range map[string]struct{ bearer, body string }{
		"no bearer":            {"", taskBody},
		"a bearer of another":  {"tok-2", taskBody},
		"a persona's token":    {"persona-token", `{"wake":{"reason":"tick"}}`},
		"a frame that is none": {"tok-1", `{"wake":{"reason":"tick"}}`},
		"a token that differs": {"tok-1", `{"task":{"key":"k","goal":"g","token":"tok-2"}}`},
		"no goal":              {"tok-1", `{"task":{"key":"k","goal":"  "}}`},
		"no key":               {"tok-1", `{"task":{"goal":"g"}}`},
	} {
		if code := postFrame(t, h, c.bearer, c.body); code < 400 {
			t.Errorf("%s was taken: %d", name, code)
		}
	}
	select {
	case <-ts.taken:
		t.Fatal("a task was taken from a request that was none")
	default:
	}
	// The verifier is not there yet: nothing is taken.
	if err := os.Rename(vf, vf+".later"); err != nil {
		t.Fatal(err)
	}
	if code := postFrame(t, h, "tok-1", taskBody); code != http.StatusUnauthorized {
		t.Fatalf("a request before the verifier was set: %d", code)
	}
	if err := os.Rename(vf+".later", vf); err != nil {
		t.Fatal(err)
	}
	if code := postFrame(t, h, "tok-1", taskBody); code != http.StatusAccepted {
		t.Fatalf("the task was refused: %d", code)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	task, err := ts.Wait(ctx)
	if err != nil || task.Key != "task-1" || task.Goal != "look at the checkout pods" || task.ReplyTo != "$req" || task.Token != "tok-1" {
		t.Fatalf("%+v %v", task, err)
	}
	// The same request again is the core retrying; a second task is refused.
	if code := postFrame(t, h, "tok-1", taskBody); code != http.StatusAccepted {
		t.Errorf("a retry of the same task: %d", code)
	}
	if code := postFrame(t, h, "tok-1", `{"task":{"key":"task-2","goal":"another"}}`); code != http.StatusConflict {
		t.Errorf("a second task: %d", code)
	}
}

func TestATaskActorAnswersTheProbeAndReportsWithItsTaskToken(t *testing.T) {
	var mu sync.Mutex
	var gotAuth, gotPath string
	var gotBody map[string]map[string]any
	hits := 0
	core := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		hits++
		gotAuth, gotPath = r.Header.Get("Authorization"), r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		_, _ = io.WriteString(w, "{}")
	}))
	defer core.Close()
	ts, _ := taskServer(t, core.URL, "tok-1")
	rec := httptest.NewRecorder()
	ts.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != 200 {
		t.Fatalf("probe %d", rec.Code)
	}
	ctx := context.Background()
	if err := ts.ReportWake(ctx, Wake{Outcome: "turn"}); err == nil {
		t.Fatal("a report was sent before a task came")
	}
	postFrame(t, ts.Handler(), "tok-1", taskBody)
	if err := ts.ReportWake(ctx, Wake{TS: 5, Reason: "task: look", Lane: "task", Outcome: "turn"}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if gotPath != LinkReportPath || gotAuth != "Bearer tok-1" || gotBody["wake"]["key"] != "task-1" || gotBody["wake"]["outcome"] != "turn" {
		t.Fatalf("%s %s %v", gotPath, gotAuth, gotBody)
	}
}

func TestATaskActorDoesNotAskAgainWhenTheCoreRefusesItsToken(t *testing.T) {
	n := 0
	core := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n++
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer core.Close()
	ts, _ := taskServer(t, core.URL, "tok-1")
	postFrame(t, ts.Handler(), "tok-1", taskBody)
	if err := ts.ReportWake(context.Background(), Wake{Outcome: "turn"}); err == nil || n != 1 {
		t.Fatalf("err %v after %d calls", err, n)
	}
}
