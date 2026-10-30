package triggers

// The link of a task actor to the core (the core's ADR-109). The core makes an
// actor for one piece of work and sends it one request; the actor never joins,
// never says hello and holds no schedule:
//
//	core → task   POST /frame    {"task":{"key","goal","replyTo","token"}}   bearer: the task's token
//	core → task   GET  /readyz   the actor's wake-up probe
//	task → core   POST /v2/messenger/spot/link/report   one wake report carrying the key
//	                                                    (bearer: the task's token)
//
// The token is an access token for this task alone. The core hands the actor
// only its SHA-256 beforehand (a verifier, not a secret): the first request
// whose bearer hashes to it is the task, and no other request is taken. The
// token is the bearer of the task's own calls to the core, for the tools and
// for the report, and nothing else: the core refuses it on hello, stats,
// schedule and control answers, and on any other workspace.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

// Task is what the core sends a task actor in its one request.
type Task struct {
	Key     string `json:"key"`
	Goal    string `json:"goal"`
	ReplyTo string `json:"replyTo"`
	Token   string `json:"token"`
}

// maxTaskGoal bounds the goal the core may send.
const maxTaskGoal = 8000

// TaskServer is the HTTP side of a task actor.
type TaskServer struct {
	base         string
	verifierFile string
	log          *slog.Logger
	hc           *http.Client

	mu    sync.Mutex
	task  *Task
	taken chan struct{}
}

// NewTaskServer builds the task actor's server for the core's API base URL. The
// verifier is read from verifierFile at every request, not now: the core sets
// it for the actor, and the process may be started before it is there. A
// request is refused until it is.
func NewTaskServer(apiURL, verifierFile string, log *slog.Logger) (*TaskServer, error) {
	u, err := url.Parse(strings.TrimRight(apiURL, "/"))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("core API URL %q must be http:// or https://", apiURL)
	}
	if strings.TrimSpace(verifierFile) == "" {
		return nil, errors.New("a task actor needs the file holding its task verifier")
	}
	return &TaskServer{
		base: u.String(), verifierFile: verifierFile, log: log,
		hc: &http.Client{Timeout: 30 * time.Second}, taken: make(chan struct{}),
	}, nil
}

// Handler serves the probe and the one request.
func (t *TaskServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("POST /frame", t.serveFrame)
	return mux
}

// verifier reads the SHA-256 the core set for this actor.
func (t *TaskServer) verifier() ([]byte, error) {
	b, err := os.ReadFile(t.verifierFile)
	if err != nil {
		return nil, err
	}
	v, err := hex.DecodeString(strings.TrimSpace(string(b)))
	if err != nil || len(v) != sha256.Size {
		return nil, errors.New("the task verifier is not a SHA-256 in hex")
	}
	return v, nil
}

func (t *TaskServer) serveFrame(w http.ResponseWriter, r *http.Request) {
	bearer, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || bearer == "" {
		http.Error(w, "not the core", http.StatusUnauthorized)
		return
	}
	want, err := t.verifier()
	if err != nil {
		t.log.Warn("a request came before the task verifier was readable", "err", err)
		http.Error(w, "not the core", http.StatusUnauthorized)
		return
	}
	sum := sha256.Sum256([]byte(bearer))
	if subtle.ConstantTimeCompare(sum[:], want) != 1 {
		http.Error(w, "not the core", http.StatusUnauthorized)
		return
	}
	var f struct {
		Task *Task `json:"task"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxFrameBody)).Decode(&f); err != nil || f.Task == nil {
		http.Error(w, "not a task", http.StatusBadRequest)
		return
	}
	task := *f.Task
	if task.Token != "" && task.Token != bearer {
		http.Error(w, "the token in the task is not the one it came with", http.StatusBadRequest)
		return
	}
	task.Token = bearer
	if strings.TrimSpace(task.Key) == "" || strings.TrimSpace(task.Goal) == "" || len(task.Goal) > maxTaskGoal {
		http.Error(w, "a task needs a key and a goal of at most 8000 bytes", http.StatusBadRequest)
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	switch {
	case t.task == nil:
		t.task = &task
		close(t.taken)
	case t.task.Key != task.Key:
		http.Error(w, "this actor has its task", http.StatusConflict)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_, _ = io.WriteString(w, "{}\n")
}

// Wait blocks until the task has come, or ctx ends.
func (t *TaskServer) Wait(ctx context.Context) (Task, error) {
	select {
	case <-t.taken:
		t.mu.Lock()
		defer t.mu.Unlock()
		return *t.task, nil
	case <-ctx.Done():
		return Task{}, ctx.Err()
	}
}

// ReportWake sends the task's one wake report, with the task's token. The core
// ends the actor when it has it, so nothing is sent after it. A report the core
// could not take is tried again a few times.
func (t *TaskServer) ReportWake(ctx context.Context, w Wake) error {
	t.mu.Lock()
	task := t.task
	t.mu.Unlock()
	if task == nil {
		return errors.New("no task has come")
	}
	if w.Key == "" {
		w.Key = task.Key
	}
	b, err := json.Marshal(map[string]any{"wake": w})
	if err != nil {
		return err
	}
	var last error
	for i := range 4 {
		if i > 0 {
			select {
			case <-time.After(time.Duration(i) * 2 * time.Second):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.base+LinkReportPath, bytes.NewReader(b))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+task.Token)
		resp, err := t.hc.Do(req)
		if err != nil {
			last = err
			continue
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		_ = resp.Body.Close()
		if resp.StatusCode/100 == 2 {
			return nil
		}
		last = fmt.Errorf("the core answered %s", resp.Status)
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			return last // the core ended or refused the task: no use in asking again
		}
	}
	return last
}
