package triggers

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/triageagent-dev/spot/internal/identity"
)

// linkCore is the core's side of the link as its ADR-107 describes it: a
// challenge, a join (pending the first time), a hello that returns the token and
// the actor's name, and a report route behind the token.
type linkCore struct {
	*fakeCore
	lmu     sync.Mutex
	posts   []string         // "kind" of each /link post, then "report" for each report
	reports []map[string]any // bodies of the reports, in order
	hellos  []map[string]any
	unauth  bool // reports are refused 401
	life    time.Duration
	// enrollHeaders is the enrollment header of each post to the link route, in order.
	enrollHeaders []string
	// bodyKeys is the enrollKey each join carried in its body.
	bodyKeys []string
	// refuseHello is how many hellos are refused 403, as for a certificate the core does not take.
	refuseHello int
	// tasks are the bodies of the task requests the core took; noTasks refuses them 403.
	tasks   []map[string]any
	noTasks bool
}

func newLinkCore(t *testing.T) *linkCore {
	return &linkCore{fakeCore: newFakeCore(t), life: 10 * time.Minute}
}

func (c *linkCore) reportsSeen() []map[string]any {
	c.lmu.Lock()
	defer c.lmu.Unlock()
	return append([]map[string]any(nil), c.reports...)
}

func (c *linkCore) handler(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case LinkReportPath:
		if r.Header.Get("Authorization") != "Bearer spt1.token-example-spot" {
			http.Error(w, `{"error":"token"}`, http.StatusUnauthorized)
			return
		}
		c.lmu.Lock()
		unauth := c.unauth
		var m map[string]any
		_ = json.Unmarshal(body, &m)
		if !unauth {
			c.reports = append(c.reports, m)
			c.posts = append(c.posts, "report")
		}
		c.lmu.Unlock()
		if unauth {
			http.Error(w, `{"error":"token"}`, http.StatusUnauthorized)
			return
		}
		_, _ = io.WriteString(w, `{"budget":{"spent":5,"cap":100,"remaining":95}}`)
	case LinkTasksPath:
		if r.Header.Get("Authorization") != "Bearer spt1.token-example-spot" {
			http.Error(w, `{"error":"token"}`, http.StatusUnauthorized)
			return
		}
		c.lmu.Lock()
		defer c.lmu.Unlock()
		if c.noTasks {
			http.Error(w, `{"error":"no"}`, http.StatusForbidden)
			return
		}
		var m map[string]any
		_ = json.Unmarshal(body, &m)
		c.tasks = append(c.tasks, m)
		_, _ = io.WriteString(w, `{"key":"task-key-1"}`)
	case LinkPath:
		c.lmu.Lock()
		c.enrollHeaders = append(c.enrollHeaders, r.Header.Get(EnrollKeyHeader))
		c.lmu.Unlock()
		c.link(w, body)
	default:
		http.NotFound(w, r)
	}
}

func (c *linkCore) link(w http.ResponseWriter, body []byte) {
	var in struct {
		Kind      string `json:"kind"`
		Nonce     []byte `json:"nonce"`
		CoreNonce []byte `json:"coreNonce"`
		Join      struct {
			CSR, Cert, Proof []byte
			EnrollKey        string `json:"enrollKey"`
		} `json:"join"`
		Hello struct {
			Spot, Workspace, Version, Runtime string
			Cert, Proof                       []byte
		} `json:"hello"`
	}
	if err := json.Unmarshal(body, &in); err != nil {
		http.Error(w, "json", http.StatusBadRequest)
		return
	}
	c.lmu.Lock()
	c.posts = append(c.posts, in.Kind)
	c.lmu.Unlock()
	nonce := []byte("core-nonce-0123456789")
	switch in.Kind {
	case "challenge":
		_ = json.NewEncoder(w).Encode(map[string]any{"kind": "challenge", "challenge": map[string]any{
			"nonce": nonce, "caCert": c.caDER, "agentNonce": in.Nonce,
			"sig": ed25519.Sign(c.caPriv, identity.ProofMessage("challenge", nonce, base64Std(in.Nonce))),
		}})
	case "join", "renew":
		csr, err := x509.ParseCertificateRequest(in.Join.CSR)
		if err != nil || csr.CheckSignature() != nil {
			c.t.Errorf("bad CSR: %v", err)
			return
		}
		pub := csr.PublicKey.(ed25519.PublicKey)
		if !ed25519.Verify(pub, identity.ProofMessage(in.Kind, nonce, identity.CSRHash(in.Join.CSR)), in.Join.Proof) {
			c.t.Errorf("%s proof does not verify", in.Kind)
			return
		}
		c.lmu.Lock()
		c.bodyKeys = append(c.bodyKeys, in.Join.EnrollKey)
		c.lmu.Unlock()
		c.mu.Lock()
		c.joins++
		pend := in.Kind == "join" && c.joins <= c.pending
		c.mu.Unlock()
		if pend {
			_ = json.NewEncoder(w).Encode(map[string]any{"kind": "join_result", "joinResult": map[string]any{"status": "pending"}})
			return
		}
		var uris []*url.URL
		uris = append(uris, csr.URIs...)
		tpl := &x509.Certificate{
			SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: csr.Subject,
			NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(24 * time.Hour), URIs: uris,
		}
		der, err := x509.CreateCertificate(rand.Reader, tpl, c.caCert, pub, c.caPriv)
		if err != nil {
			c.t.Error(err)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"kind": "join_result", "joinResult": map[string]any{"status": "issued", "cert": der, "caCert": c.caDER}})
	case "hello":
		cert, err := x509.ParseCertificate(in.Hello.Cert)
		if err != nil {
			http.Error(w, "cert", http.StatusBadRequest)
			return
		}
		if !ed25519.Verify(cert.PublicKey.(ed25519.PublicKey), identity.ProofMessage("hello", nonce, in.Hello.Spot, in.Hello.Workspace), in.Hello.Proof) {
			c.t.Error("hello proof does not verify")
			return
		}
		c.lmu.Lock()
		c.hellos = append(c.hellos, map[string]any{"spot": in.Hello.Spot, "workspace": in.Hello.Workspace, "runtime": in.Hello.Runtime})
		refuse := c.refuseHello > 0
		if refuse {
			c.refuseHello--
		}
		c.lmu.Unlock()
		if refuse {
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]any{"kind": "join_result", "joinResult": map[string]any{"status": "refused", "message": "the certificate is not valid"}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"kind": "hello", "name": "Example", "budget": map[string]any{"spent": 1, "cap": 100, "remaining": 99},
			"token": map[string]any{"value": "spt1.token-" + in.Hello.Spot, "exp": time.Now().Add(c.life).Unix()},
			"actor": "spots/s0123",
		})
	}
}

func base64Std(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

func linkClient(t *testing.T, c *linkCore, dir string) (*Link, string) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(c.handler))
	t.Cleanup(srv.Close)
	id, err := identity.Load(dir, "example-spot", "ws-1", identity.Fingerprint(c.caPub))
	if err != nil {
		t.Fatal(err)
	}
	tok := filepath.Join(dir, "token")
	l, err := NewLink(srv.URL, Cert{ID: id, Name: "example-spot", Workspace: "ws-1", Version: "test", TokenFile: tok}, quiet())
	if err != nil {
		t.Fatal(err)
	}
	return l, tok
}

func upLink(t *testing.T, c *linkCore) (*Link, string, context.CancelFunc) {
	t.Helper()
	return upLinkWith(t, c, nil)
}

// upLinkWith sets the link up before it runs, so what it holds is there at hello.
func upLinkWith(t *testing.T, c *linkCore, setup func(*Link)) (*Link, string, context.CancelFunc) {
	t.Helper()
	l, tok := linkClient(t, c, t.TempDir())
	if setup != nil {
		setup(l)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	runUntilCleanup(t, cancel, ctx, l.Run)
	wctx, wcancel := context.WithTimeout(ctx, 20*time.Second)
	defer wcancel()
	if err := l.WaitReady(wctx); err != nil {
		t.Fatalf("never ready: %v (%v)", err, c.posts)
	}
	wait(t, "connected", l.Connected)
	return l, tok, cancel
}

func TestAnActorJoinsOverHTTPAndGetsItsTokenNameAndBudget(t *testing.T) {
	c := newLinkCore(t)
	l, tok, _ := upLink(t, c)

	if b, _ := os.ReadFile(tok); string(b) != "spt1.token-example-spot" {
		t.Fatalf("token file: %q", b)
	}
	if fi, _ := os.Stat(tok); fi.Mode().Perm() != 0o600 {
		t.Fatalf("token mode %v", fi.Mode())
	}
	c.lmu.Lock()
	got := strings.Join(c.posts, ",")
	h := c.hellos[0]
	c.lmu.Unlock()
	// challenge, join (pending), challenge, join (issued), challenge, hello.
	if got != "challenge,join,challenge,join,challenge,hello" {
		t.Fatalf("exchanges: %s", got)
	}
	if h["spot"] != "example-spot" || h["workspace"] != "ws-1" || h["runtime"] != "substrate" {
		t.Fatalf("hello: %v", h)
	}
	if l.Actor() != "spots/s0123" {
		t.Errorf("actor %q", l.Actor())
	}
	p := l.Take()
	if p.Name != "Example" || !p.Budget || p.Hello {
		t.Errorf("hello must give the name and the budget and not say that everything changed: %+v", p)
	}
	if b, ok := l.Budget(); !ok || b.Cap != 100 {
		t.Errorf("budget %+v %v", b, ok)
	}
}

func TestAnActorHoldsNothingBackButTheReportsAreSentLastWhenIdle(t *testing.T) {
	c := newLinkCore(t)
	l, _, _ := upLinkWith(t, c, func(l *Link) { l.SetSchedule(Schedule{Next: 5}) })
	// The schedule went with hello; the rest waits for idle.
	wait(t, "the schedule", func() bool { return len(c.reportsSeen()) == 1 })
	l.ReportWake(Wake{Reason: "tick", Outcome: "turn", Next: 1000})
	l.ReportTurn(Turn{Model: "m", Input: 10})
	l.ReportStats(Stats{Turns: 2})
	l.ReportWake(Wake{Reason: "tick", Outcome: "turn", Key: "t-1"})

	// Nothing leaves before the supervisor says it is idle.
	time.Sleep(50 * time.Millisecond)
	if n := len(c.reportsSeen()); n != 1 {
		t.Fatalf("%d reports went out before idle: %v", n, c.reportsSeen())
	}
	l.Idle()
	var keys []string
	for _, r := range c.reportsSeen() {
		for k := range r {
			keys = append(keys, k)
		}
	}
	// The schedule went after hello; then turn, stats, and the wakes last.
	if got := strings.Join(keys, ","); got != "schedule,turn,stats,wake,wake" {
		t.Fatalf("order of reports: %s", got)
	}
	if b, ok := l.Budget(); !ok || b.Cap != 100 || b.Remaining != 95 {
		t.Errorf("the report's response carries the budget: %+v", b)
	}
	// Nothing is sent twice.
	l.Idle()
	if n := len(c.reportsSeen()); n != 5 {
		t.Errorf("%d reports after a second idle", n)
	}
}

func TestAReportTheCoreRefusesIsKeptAndTheActorJoinsAgain(t *testing.T) {
	c := newLinkCore(t)
	l, _, _ := upLink(t, c)
	c.lmu.Lock()
	c.unauth = true
	c.lmu.Unlock()
	l.ReportWake(Wake{Reason: "tick", Outcome: "turn"})
	l.Idle()
	if l.Connected() {
		// the 401 drops the link at once
		wait(t, "disconnect", func() bool { return !l.Connected() })
	}
	c.lmu.Lock()
	c.unauth = false
	c.lmu.Unlock()
	wait(t, "reconnected", l.Connected)
	l.Idle()
	if got := c.reportsSeen(); len(got) == 0 || got[len(got)-1]["wake"] == nil {
		t.Fatalf("the wake report was lost: %v", got)
	}
}

func TestTheActorSaysHelloAgainBeforeTheTokenEndsAndKeepsTheLinkUp(t *testing.T) {
	c := newLinkCore(t)
	c.life = 8 * time.Second // renewed a few seconds before its end, never sooner than 5 s after the last hello
	l, _, _ := upLink(t, c)
	waitFor(t, 12*time.Second, "a second hello", func() bool {
		c.lmu.Lock()
		defer c.lmu.Unlock()
		return len(c.hellos) >= 2
	})
	if !l.Connected() {
		t.Error("the link dropped while the token was renewed")
	}
	for _, r := range c.reportsSeen() {
		if len(r) == 0 {
			t.Error("an empty report was sent: the core does not renew a token that way")
		}
	}
}

func TestAHelloTheCoreRefusesForTheCertificateMakesTheActorJoinAfresh(t *testing.T) {
	c := newLinkCore(t)
	c.refuseHello = 1
	l, _, _ := upLink(t, c)
	c.lmu.Lock()
	got := strings.Join(c.posts, ",")
	c.lmu.Unlock()
	// The first hello was refused 403: the certificate is dropped, the join is done again.
	if !strings.Contains(got, "hello,challenge,join") || !l.Connected() {
		t.Fatalf("exchanges: %s", got)
	}
}

func TestAFrameThatMakesNoWakeStillGetsAnIdleReportThatLeavesThePlan(t *testing.T) {
	c := newLinkCore(t)
	l, _, _ := upLink(t, c)
	if r := frameReq(t, l, "/frame", `{"rooms":["team"]}`); r.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d", r.StatusCode)
	}
	l.Idle()
	var got []map[string]any
	for _, r := range c.reportsSeen() {
		if r["wake"] != nil {
			got = append(got, r)
		}
	}
	if len(got) != 1 {
		t.Fatalf("wake reports: %v", c.reportsSeen())
	}
	w := got[0]["wake"].(map[string]any)
	if w["outcome"] != "idle" || w["lane"] != "long" || w["next"] != nil || w["nextSource"] != nil || w["promise"] != nil {
		t.Errorf("an idle report must not carry a plan: %v", w)
	}
	// Once answered it is not owed again; and a real wake report answers it instead.
	l.Idle()
	if n := len(c.reportsSeen()); n != 1 {
		t.Errorf("%d reports after the second idle", n)
	}
	_ = frameReq(t, l, "/frame", `{"wake":{"reason":"tick"}}`)
	l.ReportWake(Wake{Reason: "tick", Lane: "long", Outcome: "turn", Next: 99})
	l.Idle()
	var idles int
	for _, r := range c.reportsSeen() {
		if w, ok := r["wake"].(map[string]any); ok && w["outcome"] == "idle" {
			idles++
		}
	}
	if idles != 1 {
		t.Errorf("a wake that ran was answered with an idle report too: %v", c.reportsSeen())
	}
}

func TestAHelloAloneOwesNoReportBecauseItWouldEndTheWakeTheCoreJustSent(t *testing.T) {
	c := newLinkCore(t)
	l, _, _ := upLink(t, c)
	l.Idle()
	for _, r := range c.reportsSeen() {
		if r["wake"] != nil {
			t.Errorf("a report went out after hello alone: %v", r)
		}
	}
}

func waitFor(t *testing.T, d time.Duration, what string, ok func() bool) {
	t.Helper()
	for end := time.Now().Add(d); !ok(); {
		if time.Now().After(end) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func frameReq(t *testing.T, l *Link, path, body string) *http.Response {
	t.Helper()
	return frameReqAs(t, l, path, body, "spt1.token-example-spot")
}

// frameReqAs sends a request as a client that holds token ("" = none).
func frameReqAs(t *testing.T, l *Link, path, body, token string) *http.Response {
	t.Helper()
	srv := httptest.NewServer(l.Handler())
	t.Cleanup(srv.Close)
	method := http.MethodPost
	if path == "/readyz" {
		method = http.MethodGet
	}
	req, _ := http.NewRequest(method, srv.URL+path, bytes.NewBufferString(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestReadyzAnswersAsSoonAsTheServerListensAndTheLinkStartsWithTheFirstRequest(t *testing.T) {
	c := newLinkCore(t)
	dir := t.TempDir()
	srv := httptest.NewServer(http.HandlerFunc(c.handler))
	t.Cleanup(srv.Close)
	tok := filepath.Join(dir, "token")
	opened := make(chan struct{}, 4)
	l, err := NewLink(srv.URL, Cert{Name: "example-spot", Workspace: "ws-1", Version: "test", TokenFile: tok,
		Open: func() (*identity.Identity, error) {
			opened <- struct{}{}
			return identity.Load(dir, "example-spot", "ws-1", identity.Fingerprint(c.caPub))
		}}, quiet())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		l.Wait() // the Run the first request started writes to the temporary directory
	})
	l.Serve(ctx)

	// A process booted only to be snapshotted makes no key and does not join.
	if r := frameReq(t, l, "/readyz", ""); r.StatusCode != http.StatusOK {
		t.Errorf("readyz before any request: %d", r.StatusCode)
	}
	time.Sleep(100 * time.Millisecond)
	select {
	case <-opened:
		t.Fatal("the identity was opened before the core asked for anything")
	default:
	}
	if _, err := os.Stat(filepath.Join(dir, "key")); err == nil {
		t.Fatal("a key was made at boot")
	}
	if len(c.posts) != 0 {
		t.Fatalf("the actor spoke to the core at boot: %v", c.posts)
	}

	// The first request has no token to show: it is a bare poke, accepted at once,
	// and the join and hello follow.
	if r := frameReqAs(t, l, "/frame", `{"wake":{"reason":"tick","why":"first"}}`, ""); r.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d", r.StatusCode)
	}
	wctx, wcancel := context.WithTimeout(ctx, 20*time.Second)
	defer wcancel()
	if err := l.WaitReady(wctx); err != nil {
		t.Fatalf("never ready: %v", err)
	}
	wait(t, "connected", l.Connected)
	if p := l.Take(); p.Order == nil || p.Order.Reason != "tick" || p.Order.Why != "" {
		t.Errorf("a poke without the token must ask for a plain tick, with none of its words: %+v", p)
	}
}

func TestAFrameNeedsTheAccessTokenOnceTheLinkIsUp(t *testing.T) {
	l, _, _ := upLink(t, newLinkCore(t))
	_ = l.Take()
	for name, tok := range map[string]string{"none": "", "wrong": "spt1.token-other", "prefix": "spt1.token-example-spo"} {
		r := frameReqAs(t, l, "/frame", `{"wake":{"reason":"tick","why":"forged"}}`, tok)
		if r.StatusCode != http.StatusUnauthorized {
			t.Errorf("%s: status %d", name, r.StatusCode)
		}
	}
	r := frameReqAs(t, l, "/frame", `{"control":{"id":"1","op":"wake","why":"forged"}}`, "")
	if r.StatusCode != http.StatusUnauthorized {
		t.Errorf("control with no token: status %d", r.StatusCode)
	}
	if p := l.Take(); p.Order != nil || p.Manual != nil {
		t.Errorf("a forged frame ordered a wake: %+v", p)
	}
	if r := frameReq(t, l, "/frame", `{"wake":{"reason":"tick","why":"genuine"}}`); r.StatusCode != http.StatusAccepted {
		t.Errorf("the core's own frame: status %d", r.StatusCode)
	}
}

func TestAFreshProcessTakesTheTokenItsPredecessorSaved(t *testing.T) {
	c := newLinkCore(t)
	dir := t.TempDir()
	srv := httptest.NewServer(http.HandlerFunc(c.handler))
	t.Cleanup(srv.Close)
	tok := filepath.Join(dir, "token")
	if err := os.WriteFile(tok, []byte("spt1.token-example-spot"), 0o600); err != nil {
		t.Fatal(err)
	}
	l, err := NewLink(srv.URL, Cert{Name: "example-spot", Workspace: "ws-1", Version: "test", TokenFile: tok,
		Open: func() (*identity.Identity, error) {
			return identity.Load(dir, "example-spot", "ws-1", identity.Fingerprint(c.caPub))
		}}, quiet())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		l.Wait()
	})
	l.Serve(ctx)
	if r := frameReq(t, l, "/frame", `{"wake":{"reason":"tick","why":"first"}}`); r.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d", r.StatusCode)
	}
	if p := l.Take(); p.Order == nil || p.Order.Why != "first" {
		t.Errorf("the core's frame with the saved token was dropped: %+v", p)
	}
}

func TestOneRequestOfTheCoreIsAnsweredAtATime(t *testing.T) {
	l, _, _ := upLink(t, newLinkCore(t))
	l.frameSlot <- struct{}{}
	if r := frameReq(t, l, "/frame", `{}`); r.StatusCode != http.StatusTooManyRequests {
		t.Errorf("a second request: status %d", r.StatusCode)
	}
	<-l.frameSlot
}

func TestABudgetOrNameTheCoreCouldNotHaveSentIsDropped(t *testing.T) {
	l, _, _ := upLink(t, newLinkCore(t))
	_ = l.Take()
	frameReq(t, l, "/frame", `{"budget":{"own":9000000000000000000},"name":"bad\u0000name"}`)
	p := l.Take()
	if p.Budget || p.Name != "" {
		t.Errorf("an absurd budget or a name with a control character was kept: %+v", p)
	}
	frameReq(t, l, "/frame", `{"budget":{"own":10,"cap":100,"remaining":90},"name":"Example"}`)
	if p := l.Take(); !p.Budget || p.Name != "Example" {
		t.Errorf("a plain budget and name were not kept: %+v", p)
	}
}

func TestAWakeFrameIsAcceptedAtOnceAndWakesTheLoop(t *testing.T) {
	l, _, _ := upLink(t, newLinkCore(t))
	_ = l.Take()
	r := frameReq(t, l, "/frame", `{"wake":{"reason":"tick","why":"look at the checkout errors"}}`)
	if r.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d", r.StatusCode)
	}
	select {
	case <-l.C():
	case <-time.After(2 * time.Second):
		t.Fatal("the loop was not signalled")
	}
	p := l.Take()
	if p.Order == nil || p.Order.Reason != "tick" || p.Order.Why != "look at the checkout errors" {
		t.Errorf("order: %+v", p.Order)
	}
	if p.Hello || p.Issues {
		t.Errorf("a frame from the core is not a hello: %+v", p)
	}
}

func TestRoomsAndAPromiseFrameAreKeptAndAPromiseOutranksATick(t *testing.T) {
	l, _, _ := upLink(t, newLinkCore(t))
	_ = l.Take()
	for _, b := range []string{`{"rooms":["!a:x"]}`, `{"wake":{"reason":"promise","why":"p"}}`, `{"wake":{"reason":"tick","why":"t"}}`} {
		if r := frameReq(t, l, "/frame", b); r.StatusCode != http.StatusAccepted {
			t.Fatalf("%s: %d", b, r.StatusCode)
		}
	}
	p := l.Take()
	if len(p.Rooms) != 1 || p.Order == nil || p.Order.Reason != "promise" {
		t.Errorf("%+v", p)
	}
}

func TestAControlIsAnsweredInTheResponseAndAWakeControlAsksForAWake(t *testing.T) {
	l, _, _ := upLink(t, newLinkCore(t))
	_ = l.Take()
	l.SetControl(func(_ context.Context, c Control) ControlResult {
		return ControlResult{ID: c.ID, OK: true, Msg: "did " + c.Op, Tasks: []TaskLine{{Key: "k", State: "running"}}}
	})
	var out struct {
		ControlResult ControlResult `json:"controlResult"`
	}
	r := frameReq(t, l, "/frame", `{"control":{"id":"c1","op":"stop","task":"k"}}`)
	if r.StatusCode != http.StatusOK {
		t.Fatalf("status %d", r.StatusCode)
	}
	_ = json.NewDecoder(r.Body).Decode(&out)
	if !out.ControlResult.OK || out.ControlResult.ID != "c1" || out.ControlResult.Msg != "did stop" {
		t.Errorf("%+v", out)
	}

	r = frameReq(t, l, "/frame", `{"control":{"id":"c2","op":"wake","why":"a person asked"}}`)
	out.ControlResult = ControlResult{}
	_ = json.NewDecoder(r.Body).Decode(&out)
	if !out.ControlResult.OK || out.ControlResult.Msg != "wake asked" || len(out.ControlResult.Tasks) != 1 {
		t.Errorf("wake control: %+v", out)
	}
	if p := l.Take(); p.Manual == nil || p.Manual.Why != "a person asked" {
		t.Errorf("manual wake: %+v", p.Manual)
	}
}

func TestABadFrameIsRefused(t *testing.T) {
	l, _, _ := upLink(t, newLinkCore(t))
	if r := frameReq(t, l, "/frame", `not json`); r.StatusCode != http.StatusBadRequest {
		t.Errorf("garbage: %d", r.StatusCode)
	}
	if r := frameReq(t, l, "/frame", `{"control":{"op":"list"}}`); r.StatusCode != http.StatusBadRequest {
		t.Errorf("a control with no id: %d", r.StatusCode)
	}
	if r := frameReq(t, l, "/frame", `{"wake":{"reason":"tick","why":"`+strings.Repeat("x", maxFrameBody)+`"}}`); r.StatusCode != http.StatusBadRequest {
		t.Errorf("a frame over the limit: %d", r.StatusCode)
	}
}

func TestAnActorWithAnInjectedKeySendsAPlaceholderHeaderOnTheJoinOnly(t *testing.T) {
	c := newLinkCore(t)
	srv := httptest.NewServer(http.HandlerFunc(c.handler))
	t.Cleanup(srv.Close)
	dir := t.TempDir()
	id, err := identity.Load(dir, "example-spot", "ws-1", identity.Fingerprint(c.caPub))
	if err != nil {
		t.Fatal(err)
	}
	l, err := NewLink(srv.URL, Cert{ID: id, Name: "example-spot", Workspace: "ws-1", Version: "test",
		TokenFile: filepath.Join(dir, "token"), EnrollKeyInjected: true}, quiet())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	runUntilCleanup(t, cancel, ctx, l.Run)
	wctx, wcancel := context.WithTimeout(ctx, 20*time.Second)
	defer wcancel()
	if err := l.WaitReady(wctx); err != nil {
		t.Fatal(err)
	}
	c.lmu.Lock()
	defer c.lmu.Unlock()
	if len(c.posts) != len(c.enrollHeaders) {
		t.Fatalf("posts %v headers %v", c.posts, c.enrollHeaders)
	}
	var joins int
	for i, kind := range c.posts {
		want := ""
		if kind == "join" {
			want, joins = EnrollKeyPlaceholder, joins+1
		}
		if c.enrollHeaders[i] != want {
			t.Errorf("%s carried %q, want %q", kind, c.enrollHeaders[i], want)
		}
	}
	if joins == 0 {
		t.Fatal("no join was made")
	}
	for _, k := range c.bodyKeys {
		if k != "" {
			t.Errorf("the join body carried an enrollKey: %q", k)
		}
	}
}

// runUntilCleanup starts run and, at the end of the test, stops it and waits for
// it, so nothing it writes races the removal of the test's temporary directory.
func runUntilCleanup(t *testing.T, cancel context.CancelFunc, ctx context.Context, run func(context.Context)) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

func TestOnlyTheFirstPokeOfAProcessAsksForATick(t *testing.T) {
	l, err := NewLink("http://core.invalid", Cert{Name: "example-spot", Workspace: "ws-1", TokenFile: filepath.Join(t.TempDir(), "token"),
		Open: func() (*identity.Identity, error) { return nil, errors.New("not opened") }}, quiet())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if r := frameReqAs(t, l, "/frame", `{"wake":{"reason":"tick","why":"forged"}}`, ""); r.StatusCode != http.StatusAccepted {
			t.Fatalf("poke %d: status %d", i, r.StatusCode)
		}
		if i == 0 {
			if p := l.Take(); p.Order == nil || p.Order.Why != "" {
				t.Fatalf("the first poke: %+v", p)
			}
		}
	}
	if p := l.Take(); p.Order != nil {
		t.Errorf("a later poke asked for a wake: %+v", p)
	}
}

func TestASpotAsksTheCoreForATaskActorWithItsAccessToken(t *testing.T) {
	c := newLinkCore(t)
	l, _, _ := upLink(t, c)
	key, err := l.StartTask(context.Background(), "check the cart", "look at the cart pods and report")
	if err != nil || key != "task-key-1" {
		t.Fatalf("%q %v", key, err)
	}
	c.lmu.Lock()
	got := c.tasks
	c.lmu.Unlock()
	if len(got) != 1 || got[0]["title"] != "check the cart" || got[0]["goal"] != "look at the cart pods and report" {
		t.Fatalf("%v", got)
	}
	c.lmu.Lock()
	c.noTasks = true
	c.lmu.Unlock()
	if _, err := l.StartTask(context.Background(), "t", "g"); err == nil {
		t.Fatal("a task the core refused was reported as started")
	}
}
