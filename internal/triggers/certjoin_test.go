package triggers

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
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

	"github.com/gorilla/websocket"

	"github.com/triageagent-dev/spot/internal/identity"
)

// fakeCore speaks the join route as the core's ADR-105 describes it: challenge
// first, then join (pending the first time, issued after), renew, or hello.
// Every proof is checked as the core would.
type fakeCore struct {
	t      *testing.T
	caPub  ed25519.PublicKey
	caPriv ed25519.PrivateKey
	caCert *x509.Certificate
	caDER  []byte
	life   time.Duration
	// backdate is how long before now an issued certificate starts.
	backdate time.Duration

	mu      sync.Mutex
	kinds   []string // the first frame of each connection
	joins   int
	pending int      // join requests answered pending before one is issued
	revoked bool     // hello is refused with 1008
	needKey string   // when set, a join must carry this key, and then needs no approval
	keys    []string // the enroll key each join carried
	hellos  []map[string]any
}

func newFakeCore(t *testing.T) *fakeCore {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "core ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(48 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, pub, priv)
	if err != nil {
		t.Fatal(err)
	}
	c, _ := x509.ParseCertificate(der)
	return &fakeCore{t: t, caPub: pub, caPriv: priv, caCert: c, caDER: der, life: 24 * time.Hour, pending: 1}
}

func (f *fakeCore) sentKinds() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.kinds...)
}

func (f *fakeCore) handler(w http.ResponseWriter, r *http.Request) {
	agentNonce, err := base64.StdEncoding.DecodeString(r.Header.Get("X-Triage-Agent-Nonce"))
	if err != nil || len(agentNonce) != 32 {
		http.Error(w, `{"error":"nonce"}`, http.StatusBadRequest)
		return
	}
	conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()
	nonce := []byte("core-nonce-0123456789")
	_ = conn.WriteJSON(map[string]any{"v": 1, "kind": "challenge", "challenge": map[string]any{
		"nonce": nonce, "caCert": f.caDER, "agentNonce": agentNonce,
		"sig": ed25519.Sign(f.caPriv, identity.ProofMessage("challenge", nonce, base64.StdEncoding.EncodeToString(agentNonce))),
	}})
	var in struct {
		Kind string `json:"kind"`
		Join struct {
			CSR, Cert, Proof []byte
			EnrollKey        string `json:"enrollKey"`
		} `json:"join"`
		Hello struct {
			Spot, Workspace, Version string
			Cert, Proof              []byte
		} `json:"hello"`
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if err := conn.ReadJSON(&in); err != nil {
		return
	}
	f.mu.Lock()
	f.kinds = append(f.kinds, in.Kind)
	f.mu.Unlock()
	switch in.Kind {
	case "join", "renew":
		csr, err := x509.ParseCertificateRequest(in.Join.CSR)
		if err != nil || csr.CheckSignature() != nil {
			f.t.Errorf("bad CSR: %v", err)
			return
		}
		pub := csr.PublicKey.(ed25519.PublicKey)
		if !ed25519.Verify(pub, identity.ProofMessage(in.Kind, nonce, identity.CSRHash(in.Join.CSR)), in.Join.Proof) {
			f.t.Errorf("%s proof does not verify", in.Kind)
			return
		}
		f.mu.Lock()
		f.joins++
		pend := in.Kind == "join" && f.joins <= f.pending
		if in.Kind == "join" {
			f.keys = append(f.keys, in.Join.EnrollKey)
			if f.needKey != "" {
				pend = false
				if in.Join.EnrollKey != f.needKey {
					f.mu.Unlock()
					_ = conn.WriteJSON(map[string]any{"v": 1, "kind": "join_result", "joinResult": map[string]any{"status": "refused", "message": "an enrollment key is required"}})
					return
				}
			}
		}
		f.mu.Unlock()
		if pend {
			_ = conn.WriteJSON(map[string]any{"v": 1, "kind": "join_result", "joinResult": map[string]any{"status": "pending"}})
			return
		}
		var uris []*url.URL
		uris = append(uris, csr.URIs...)
		tpl := &x509.Certificate{
			SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: csr.Subject,
			NotBefore: time.Now().Add(-time.Minute - f.backdate), NotAfter: time.Now().Add(f.life), URIs: uris,
		}
		der, err := x509.CreateCertificate(rand.Reader, tpl, f.caCert, pub, f.caPriv)
		if err != nil {
			f.t.Error(err)
			return
		}
		_ = conn.WriteJSON(map[string]any{"v": 1, "kind": "join_result", "joinResult": map[string]any{"status": "issued", "cert": der, "caCert": f.caDER}})
	case "hello":
		cert, err := x509.ParseCertificate(in.Hello.Cert)
		if err != nil {
			return
		}
		if !ed25519.Verify(cert.PublicKey.(ed25519.PublicKey), identity.ProofMessage("hello", nonce, in.Hello.Spot, in.Hello.Workspace), in.Hello.Proof) {
			f.t.Error("hello proof does not verify")
			return
		}
		f.mu.Lock()
		f.hellos = append(f.hellos, map[string]any{"spot": in.Hello.Spot, "workspace": in.Hello.Workspace, "version": in.Hello.Version})
		revoked := f.revoked
		f.mu.Unlock()
		if revoked {
			_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "revoked"))
			return
		}
		_ = conn.WriteJSON(map[string]any{"token": map[string]any{"value": "spt1.token-" + in.Hello.Spot, "exp": time.Now().Add(10 * time.Minute).Unix()}})
		_ = conn.WriteJSON(map[string]any{"hello": true, "name": "Example"})
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}
}

func certClient(t *testing.T, f *fakeCore, dir string) (*Client, *identity.Identity, string) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(f.handler))
	t.Cleanup(srv.Close)
	id, err := identity.Load(dir, "example-spot", "ws-1", identity.Fingerprint(f.caPub))
	if err != nil {
		t.Fatal(err)
	}
	tok := filepath.Join(dir, "token")
	c, err := NewCert(srv.URL, Cert{ID: id, Name: "example-spot", Workspace: "ws-1", Version: "test", TokenFile: tok}, quiet())
	if err != nil {
		t.Fatal(err)
	}
	return c, id, tok
}

func TestASpotJoinsByCertificateAndGetsItsToken(t *testing.T) {
	f := newFakeCore(t)
	c, id, tok := certClient(t, f, t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runUntilCleanup(t, cancel, ctx, c.Run)

	wctx, wcancel := context.WithTimeout(ctx, 20*time.Second)
	defer wcancel()
	if err := c.WaitReady(wctx); err != nil {
		t.Fatalf("never ready: %v (kinds %v)", err, f.sentKinds())
	}
	wait(t, "connected", c.Connected)
	if b, _ := os.ReadFile(tok); string(b) != "spt1.token-example-spot" {
		t.Fatalf("token file: %q", b)
	}
	if fi, _ := os.Stat(tok); fi.Mode().Perm() != 0o600 {
		t.Fatalf("token mode %v", fi.Mode())
	}
	// join (pending), join (issued), then hello.
	if got := strings.Join(f.sentKinds(), ","); got != "join,join,hello" {
		t.Fatalf("frames: %s", got)
	}
	f.mu.Lock()
	h := f.hellos[0]
	f.mu.Unlock()
	if h["spot"] != "example-spot" || h["workspace"] != "ws-1" || h["version"] != "test" {
		t.Fatalf("hello: %v", h)
	}
	if _, usable, _ := id.CertState(time.Now()); !usable {
		t.Fatal("the certificate was not kept")
	}
	// The stream is today's: reports go up on it.
	c.ReportStats(Stats{Turns: 3})
}

func TestARestartedSpotSaysHelloWithItsCertificate(t *testing.T) {
	f := newFakeCore(t)
	dir := t.TempDir()
	c, _, _ := certClient(t, f, dir)
	ctx, cancel := context.WithCancel(context.Background())
	runUntilCleanup(t, cancel, ctx, c.Run)
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	cancel()
	wait(t, "down", func() bool { return !c.Connected() })

	c2, _, _ := certClient(t, f, dir)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	runUntilCleanup(t, cancel2, ctx2, c2.Run)
	if err := c2.WaitReady(ctx2); err != nil {
		t.Fatal(err)
	}
	kinds := f.sentKinds()
	if kinds[len(kinds)-1] != "hello" || kinds[len(kinds)-2] != "hello" {
		t.Fatalf("a restart should not join again: %v", kinds)
	}
}

func TestACertificateDueForRenewalIsRenewed(t *testing.T) {
	f := newFakeCore(t)
	f.pending = 0
	f.backdate = 60 * time.Minute // issued an hour ago, 10 minutes left: under a third of its life
	f.life = 10 * time.Minute
	dir := t.TempDir()
	c, id, _ := certClient(t, f, dir)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runUntilCleanup(t, cancel, ctx, c.Run)
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if _, usable, renew := id.CertState(time.Now()); !usable || !renew {
		t.Fatalf("test setup: usable=%v renew=%v", usable, renew)
	}
	cancel()
	wait(t, "down", func() bool { return !c.Connected() })

	f.backdate, f.life = 0, 24*time.Hour // the renewed certificate is a fresh one
	f.mu.Lock()
	f.kinds = nil
	f.mu.Unlock()
	c2, id2, _ := certClient(t, f, dir)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	runUntilCleanup(t, cancel2, ctx2, c2.Run)
	wait(t, "renew and hello", func() bool { return strings.Join(f.sentKinds(), ",") == "renew,hello" })
	if _, _, renew := id2.CertState(time.Now()); renew {
		t.Fatal("the certificate is still due after renewal")
	}
}

func TestACoreThatIsNotTheTrustedOneIsNotAnswered(t *testing.T) {
	f := newFakeCore(t)
	dir := t.TempDir()
	srv := httptest.NewServer(http.HandlerFunc(f.handler))
	defer srv.Close()
	other, _, _ := ed25519.GenerateKey(rand.Reader)
	id, err := identity.Load(dir, "example-spot", "ws-1", identity.Fingerprint(other))
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewCert(srv.URL, Cert{ID: id, Name: "example-spot", Workspace: "ws-1", TokenFile: filepath.Join(dir, "token")}, quiet())
	if err != nil {
		t.Fatal(err)
	}
	up, err := c.once(context.Background())
	if err == nil || up || !strings.Contains(err.Error(), "not the one this spot trusts") {
		t.Fatalf("up=%v err=%v", up, err)
	}
	if len(f.sentKinds()) != 0 {
		t.Fatalf("the spot sent %v to a core it does not trust", f.sentKinds())
	}
}

func TestARevokedCertificateIsDroppedAndTheSpotJoinsAfresh(t *testing.T) {
	f := newFakeCore(t)
	f.pending = 0
	dir := t.TempDir()
	c, id, _ := certClient(t, f, dir)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runUntilCleanup(t, cancel, ctx, c.Run)
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.revoked = true
	f.mu.Unlock()
	// Close the open stream from the spot's side so it reconnects with hello.
	c.wmu.Lock()
	if c.conn != nil {
		_ = c.conn.Close()
	}
	c.wmu.Unlock()
	wait(t, "the certificate to be dropped", func() bool { _, usable, _ := id.CertState(time.Now()); return !usable })
	wait(t, "a new join", func() bool {
		k := f.sentKinds()
		return len(k) >= 3 && k[len(k)-1] == "join"
	})
}

func TestCertIdentityNeedsItsParts(t *testing.T) {
	if _, err := NewCert("http://core.example:8080", Cert{}, quiet()); err == nil {
		t.Fatal("an empty identity was accepted")
	}
}

func TestASpotWithAnEnrollmentKeyIsIssuedAtOnceAndSendsItOnlyWhileItHasNoCertificate(t *testing.T) {
	f := newFakeCore(t)
	f.needKey = "tak_abc_secret"
	dir := t.TempDir()
	keyFile := filepath.Join(dir, "enroll")
	if err := os.WriteFile(keyFile, []byte("tak_abc_secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(f.handler))
	defer srv.Close()
	id, err := identity.Load(filepath.Join(dir, "id"), "example-spot", "ws-1", identity.Fingerprint(f.caPub))
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewCert(srv.URL, Cert{ID: id, Name: "example-spot", Workspace: "ws-1", Version: "test", TokenFile: filepath.Join(dir, "token"), EnrollKeyFile: keyFile}, quiet())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runUntilCleanup(t, cancel, ctx, c.Run)
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(f.sentKinds(), ","); got != "join,hello" {
		t.Fatalf("frames: %s", got)
	}
	f.mu.Lock()
	keys := append([]string(nil), f.keys...)
	f.mu.Unlock()
	if len(keys) != 1 || keys[0] != "tak_abc_secret" {
		t.Fatalf("the join carried %v", keys)
	}
}

func TestASpotWithoutTheRightKeyIsRefusedAndKeepsNoCertificate(t *testing.T) {
	f := newFakeCore(t)
	f.needKey = "tak_abc_secret"
	c, id, _ := certClient(t, f, t.TempDir())
	up, err := c.once(context.Background())
	if up || err == nil || !strings.Contains(err.Error(), "an enrollment key is required") {
		t.Fatalf("up=%v err=%v", up, err)
	}
	if _, usable, _ := id.CertState(time.Now()); usable {
		t.Fatal("a certificate was kept after a refusal")
	}
}
