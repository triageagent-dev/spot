package triggers

// The link of a substrate actor to the core (the core's ADR-107). An actor holds
// no stream: it is suspended when it is idle and no connection survives that.
// The spot talks to the core in requests instead, with the frames and the
// certificate join of the stream, and the core talks to the actor in requests to
// its port:
//
//	spot → core   POST /v2/messenger/spot/link          challenge, join, renew, hello
//	spot → core   POST /v2/messenger/spot/link/report   wake, turn, stats, schedule
//	                                                    (bearer: the token hello gave)
//	core → spot   POST /frame                           wake, rooms, name, budget, control
//	core → spot   GET  /readyz                          the actor's wake-up probe
//
// A wake is answered 202 at once and the wake report goes to the core when the
// supervisor is idle, last, after the turn's cost and the stats: the core may
// suspend the actor as soon as it has the report, so nothing the spot sends
// comes after it. A control is answered in the response.

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
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
	"sync/atomic"
	"time"

	"github.com/triageagent-dev/spot/internal/identity"
)

// Routes of the link.
const (
	LinkPath       = "/v2/messenger/spot/link"
	LinkReportPath = LinkPath + "/report"
)

// maxFrameBody bounds what the core may send in one request.
const maxFrameBody = 64 << 10

// maxLinkBuffer bounds the reports kept while the core cannot be reached.
const maxLinkBuffer = 100

// Link is the HTTP link of a substrate actor to the core. It satisfies the
// supervisor's Trigger, so the loop is the one the stream drives.
type Link struct {
	inbox
	base string
	cert Cert
	log  *slog.Logger
	hc   *http.Client

	connected atomic.Bool
	poked     atomic.Bool // a request with no token has already been answered with a tick
	ready     chan struct{}
	reauth    chan struct{}
	frameSlot chan struct{} // one request of the core at a time

	// The link starts with the first request of the core, not with the process:
	// an actor is also booted to be snapshotted before it has a wake, and the key
	// it made then would be in every actor made from that snapshot.
	startOnce sync.Once
	running   sync.WaitGroup // the Run that begin started
	runCtx    context.Context

	// Guarded by inbox.mu.
	outbox []Turn
	wakes  []Wake
	stats  *Stats
	sched  *Schedule
	// owed is set when the core sent a wake or the rooms: it marks the actor
	// running and waits for a wake report to mark it suspended.
	owed         bool
	bearer       string
	prevBearer   string // the token before the last hello: the core may still be sending it
	tokenExp     time.Time
	actor        string
	noRenewUntil time.Time
	pendingN     int
}

// NewLink builds the link for the core's HTTP API base URL. The certificate
// identity is the same as the stream's (Cert); the token the core hands over is
// written to Cert.TokenFile, where the MCP client reads it.
func NewLink(apiURL string, cert Cert, log *slog.Logger) (*Link, error) {
	if (cert.ID == nil && cert.Open == nil) || cert.Name == "" || cert.Workspace == "" || cert.TokenFile == "" {
		return nil, errors.New("a certificate identity needs an identity (or what opens it), a name, a workspace and a token file")
	}
	u, err := url.Parse(strings.TrimRight(apiURL, "/"))
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("core API URL %q must be http:// or https://", apiURL)
	}
	return &Link{
		inbox: newInbox(), base: u.String(), cert: cert, log: log,
		hc:    &http.Client{Timeout: 30 * time.Second},
		ready: make(chan struct{}), reauth: make(chan struct{}, 1), frameSlot: make(chan struct{}, 1),
	}, nil
}

// Connected reports whether the actor has joined and said hello. While it has
// not, the supervisor falls back to polling.
func (l *Link) Connected() bool { return l.connected.Load() }

// Actor is the name the core expects the actor to have, from its last hello;
// empty before the first.
func (l *Link) Actor() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.actor
}

// WaitReady blocks until the first hello is done: the spot has its access token,
// the link is connected and the schedule is with the core.
func (l *Link) WaitReady(ctx context.Context) error {
	select {
	case <-l.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ReportTurn keeps what a turn cost until the supervisor is idle.
func (l *Link) ReportTurn(t Turn) {
	l.mu.Lock()
	l.outbox = append(l.outbox, t)
	if len(l.outbox) > maxLinkBuffer {
		l.outbox = l.outbox[len(l.outbox)-maxLinkBuffer:]
	}
	l.mu.Unlock()
}

// ReportWake keeps one line of activity until the supervisor is idle.
func (l *Link) ReportWake(w Wake) {
	l.mu.Lock()
	if w.Key == "" && w.Lane != "short" {
		l.owed = false // a wake of the long lane answers the core's
	}
	l.wakes = append(l.wakes, w)
	if len(l.wakes) > maxLinkBuffer {
		l.wakes = l.wakes[len(l.wakes)-maxLinkBuffer:]
	}
	l.mu.Unlock()
}

// ReportStats keeps the snapshot; a newer one replaces it.
func (l *Link) ReportStats(s Stats) {
	l.mu.Lock()
	l.stats = &s
	l.mu.Unlock()
}

// SetSchedule sets the schedule hello is followed by.
func (l *Link) SetSchedule(s Schedule) {
	l.mu.Lock()
	l.sched = &s
	l.mu.Unlock()
}

// Idle is the supervisor saying it has nothing running and nothing waiting:
// everything kept goes to the core now, in the order the core needs. A report
// the core could not take is kept for the next time.
func (l *Link) Idle() {
	if !l.connected.Load() {
		return
	}
	l.mu.Lock()
	if l.owed && len(l.wakes) == 0 {
		// The core sent a frame that made no wake (the rooms had nothing new): it
		// still waits for a report, and a cold process holds no plan to put in it,
		// so this one says "idle" and leaves the core's plan as it is.
		l.wakes = append(l.wakes, Wake{TS: time.Now().UnixMilli(), Reason: "rooms", Lane: "long", Outcome: "idle"})
		l.owed = false
	}
	turns, wakes, st := l.outbox, l.wakes, l.stats
	l.outbox, l.wakes, l.stats = nil, nil, nil
	l.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var sent int
	for _, t := range turns {
		if l.report(ctx, map[string]any{"turn": t}) != nil {
			break
		}
		sent++
	}
	turns = turns[sent:]
	if len(turns) == 0 && st != nil && l.report(ctx, map[string]any{"stats": st}) == nil {
		st = nil
	}
	sent = 0
	if len(turns) == 0 && st == nil {
		for _, w := range wakes {
			if l.report(ctx, map[string]any{"wake": w}) != nil {
				break
			}
			sent++
		}
		wakes = wakes[sent:]
	}
	if len(turns) == 0 && st == nil && len(wakes) == 0 {
		return
	}
	l.mu.Lock()
	l.outbox = append(turns, l.outbox...)
	l.wakes = append(wakes, l.wakes...)
	if l.stats == nil {
		l.stats = st
	}
	l.mu.Unlock()
}

var errUnauthorized = errors.New("the core does not accept the access token")

// report posts one report with the access token and takes what the response
// carries: the budget, and a new token when this one is half spent.
func (l *Link) report(ctx context.Context, body map[string]any) error {
	l.mu.Lock()
	tok := l.bearer
	l.mu.Unlock()
	var out struct {
		Budget *Budget     `json:"budget"`
		Token  *tokenFrame `json:"token"`
	}
	status, err := l.post(ctx, LinkReportPath, tok, body, &out)
	switch {
	case status == http.StatusUnauthorized:
		l.connected.Store(false)
		select {
		case l.reauth <- struct{}{}:
		default:
		}
		return errUnauthorized
	case err != nil:
		return err
	}
	if out.Budget != nil || out.Token != nil {
		if out.Token != nil {
			l.takeToken(out.Token)
		}
		if out.Budget != nil {
			l.add(frame{Budget: out.Budget})
		}
	}
	return nil
}

// post sends a JSON body and decodes a JSON answer, whatever the status; err is
// set for a transport failure, an unreadable answer or a status that is neither
// 2xx nor one the caller reads (401).
func (l *Link) post(ctx context.Context, path, bearer string, in, out any) (int, error) {
	return l.postH(ctx, path, bearer, nil, in, out)
}

// postH is post with more headers.
func (l *Link) postH(ctx context.Context, path, bearer string, hdr map[string]string, in, out any) (int, error) {
	b, err := json.Marshal(in)
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, l.base+path, bytes.NewReader(b))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := l.hc.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return resp.StatusCode, err
	}
	if len(bytes.TrimSpace(raw)) > 0 && out != nil {
		if err := json.Unmarshal(raw, out); err != nil && resp.StatusCode/100 == 2 {
			return resp.StatusCode, fmt.Errorf("the core's answer is not JSON: %w", err)
		}
	}
	if resp.StatusCode/100 != 2 && resp.StatusCode != http.StatusUnauthorized && resp.StatusCode != http.StatusForbidden {
		why := strings.TrimSpace(string(raw))
		if len(why) > 200 {
			why = why[:200]
		}
		return resp.StatusCode, fmt.Errorf("the core answered %s %s", resp.Status, why)
	}
	return resp.StatusCode, nil
}

// takeToken writes the access token to its file and keeps it for the reports.
func (l *Link) takeToken(t *tokenFrame) {
	if t == nil || t.Value == "" {
		return
	}
	if err := identity.WriteFile(l.cert.TokenFile, []byte(t.Value)); err != nil {
		l.log.Error("the core's access token could not be saved", "err", err)
		return
	}
	exp := time.Unix(t.Exp, 0)
	if t.Exp > 1e11 { // milliseconds
		exp = time.UnixMilli(t.Exp)
	}
	l.mu.Lock()
	if l.bearer != "" && l.bearer != t.Value {
		l.prevBearer = l.bearer
	}
	l.bearer, l.tokenExp = t.Value, exp
	l.mu.Unlock()
}

// markReady lets WaitReady go: hello is done, the link is connected and the
// schedule is with the core.
func (l *Link) markReady() {
	select {
	case <-l.ready:
	default:
		close(l.ready)
	}
}

// Serve makes the link wait for the first request of the core: only then does it
// open the identity, join and say hello. It returns at once; ctx ends the link.
func (l *Link) Serve(ctx context.Context) {
	l.mu.Lock()
	l.runCtx = ctx
	l.mu.Unlock()
}

// begin starts the link, once, when the core's first request arrives.
func (l *Link) begin() {
	l.startOnce.Do(func() {
		l.mu.Lock()
		ctx := l.runCtx
		l.mu.Unlock()
		if ctx == nil {
			return // Run was called directly
		}
		l.running.Add(1)
		go func() {
			defer l.running.Done()
			l.Run(ctx)
		}()
	})
}

// Wait blocks until the Run that the core's first request started has returned,
// which it does once the context given to Serve ends.
func (l *Link) Wait() { l.running.Wait() }

// open loads the identity the first time it is needed.
func (l *Link) open() error {
	if l.cert.ID != nil {
		return nil
	}
	id, err := l.cert.Open()
	if err != nil {
		return err
	}
	l.cert.ID = id
	return nil
}

// Run keeps the link up until ctx ends: it joins and says hello, says hello again
// before the access token ends (the core does not renew it), and joins afresh
// when the core stops accepting the token.
func (l *Link) Run(ctx context.Context) {
	backoff := minBackoff
	for ctx.Err() == nil {
		err := l.open()
		if err == nil {
			err = l.connect(ctx)
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			l.log.Warn("the link to the core is not up; retrying", "err", err, "in", backoff.String())
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			}
			if backoff *= 2; backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}
		backoff = minBackoff
		l.add(frame{}) // what was kept while the link was down goes when the loop is idle
		for l.waitRefresh(ctx) {
			// A hello does not end the wake the core sent; it only brings a new token.
			if err := l.connect(ctx); err != nil {
				l.log.Warn("the access token could not be renewed; trying again", "err", err)
				select {
				case <-time.After(10 * time.Second):
				case <-ctx.Done():
				}
			}
		}
		l.connected.Store(false)
	}
}

// refreshBefore is how long before its end the access token is replaced.
const refreshBefore = 3 * time.Minute

// waitRefresh blocks until the token is about to end, and returns true; false
// when the core stopped accepting it or ctx ended.
func (l *Link) waitRefresh(ctx context.Context) bool {
	l.mu.Lock()
	d := time.Until(l.tokenExp) - refreshBefore
	l.mu.Unlock()
	if d < 5*time.Second {
		d = 5 * time.Second
	}
	select {
	case <-ctx.Done():
		return false
	case <-l.reauth:
		return false
	case <-time.After(d):
		return true
	}
}

type linkIn struct {
	Kind       string         `json:"kind"`
	Challenge  *wireChallenge `json:"challenge,omitempty"`
	JoinResult *wireResult    `json:"joinResult,omitempty"`
	Name       string         `json:"name,omitempty"`
	Budget     *Budget        `json:"budget,omitempty"`
	Token      *tokenFrame    `json:"token,omitempty"`
	Actor      string         `json:"actor,omitempty"`
}

// connect does what the spot's certificate calls for, in as many exchanges as
// it takes: join, or renew, then hello. It returns nil when hello was accepted.
func (l *Link) connect(ctx context.Context) error {
	id := l.cert.ID
	for range 3 {
		nonce := make([]byte, 32)
		if _, err := rand.Read(nonce); err != nil {
			return err
		}
		var in linkIn
		ctxc, cancel := context.WithTimeout(ctx, joinWait)
		_, err := l.post(ctxc, LinkPath, "", map[string]any{
			"kind": "challenge", "spot": l.cert.Name, "workspace": l.cert.Workspace, "nonce": nonce,
		}, &in)
		cancel()
		if err != nil {
			return fmt.Errorf("asking the core for a challenge: %w", err)
		}
		if in.Kind != "challenge" || in.Challenge == nil {
			return fmt.Errorf("the core's answer is %q, not a challenge", in.Kind)
		}
		ch := in.Challenge
		if err := id.VerifyChallenge(&identity.Challenge{
			CACert: ch.CACert, CrossCert: ch.CrossCert, Nonce: ch.Nonce, AgentNonce: ch.AgentNonce, Sig: ch.Sig, ChannelBound: ch.ChannelBound,
		}, identity.Binding{SentNonce: nonce}); err != nil {
			return fmt.Errorf("the core is not the one this spot trusts: %w", err)
		}

		cert, usable, renew := id.CertState(time.Now())
		switch {
		case !usable:
			if err := l.join(ctx, ch.Nonce); err != nil && !errors.Is(err, errJoinIssued) {
				return err
			}
			continue // issued: a new challenge, then hello
		case renew && time.Now().After(l.noRenewUntil):
			if err := l.renew(ctx, ch.Nonce, cert); err != nil && !errors.Is(err, errJoinIssued) {
				l.noRenewUntil = time.Now().Add(renewRetry)
				l.log.Warn("certificate renewal failed; the current certificate is used until the next try", "err", err, "retryIn", renewRetry.String())
			}
			continue
		}
		return l.hello(ctx, ch.Nonce, cert)
	}
	return errors.New("the core did not take the spot's hello after a join")
}

// hello says who the actor is and takes the token, the name and the budget.
func (l *Link) hello(ctx context.Context, coreNonce, cert []byte) error {
	id := l.cert.ID
	var out linkIn
	ctxc, cancel := context.WithTimeout(ctx, joinWait)
	defer cancel()
	status, err := l.post(ctxc, LinkPath, "", map[string]any{
		"kind": "hello", "coreNonce": coreNonce,
		"hello": map[string]any{
			"spot": l.cert.Name, "workspace": l.cert.Workspace, "version": l.cert.Version, "runtime": "substrate", "cert": cert,
			"proof": id.Sign(identity.ProofMessage("hello", coreNonce, l.cert.Name, l.cert.Workspace)),
		},
	}, &out)
	if status == http.StatusUnauthorized || status == http.StatusForbidden {
		why := "refused"
		if out.JoinResult != nil {
			why = strings.TrimSpace(out.JoinResult.Status + " " + out.JoinResult.Message)
		}
		if status == http.StatusForbidden {
			id.DropCert() // the core does not accept the certificate: join afresh
		}
		return fmt.Errorf("the core refused the hello: %s", why)
	}
	if err != nil {
		return err
	}
	if out.Kind != "hello" || out.Token == nil || out.Token.Value == "" {
		return fmt.Errorf("the core's answer to hello is %q with no token", out.Kind)
	}
	l.mu.Lock()
	l.actor = out.Actor
	l.mu.Unlock()
	l.takeToken(out.Token)
	l.add(frame{Name: out.Name, Budget: out.Budget})
	l.connected.Store(true)
	l.log.Info("the link to the core is up", "actor", out.Actor)
	l.sendSchedule(ctx)
	l.markReady()
	return nil
}

// sendSchedule hands the core the schedule, once per hello.
func (l *Link) sendSchedule(ctx context.Context) {
	l.mu.Lock()
	s := l.sched
	l.mu.Unlock()
	// A process started for a wake knows no plan, and a schedule with none would
	// replace the one the core holds.
	if s != nil && (s.Next != 0 || s.Promise != nil) {
		_ = l.report(ctx, map[string]any{"schedule": s})
	}
}

func (l *Link) join(ctx context.Context, coreNonce []byte) error {
	id := l.cert.ID
	csr, err := id.CSR()
	if err != nil {
		return err
	}
	body := map[string]any{"csr": csr, "proof": id.Sign(identity.ProofMessage("join", coreNonce, identity.CSRHash(csr)))}
	if l.cert.EnrollKeyInjected {
		// The key is not here: the egress puts it in this header from a Secret.
		return l.joinExchange(ctx, "join", coreNonce, body, map[string]string{EnrollKeyHeader: EnrollKeyPlaceholder})
	}
	if k := l.enrollKey(); k != "" {
		body["enrollKey"] = k
	}
	return l.joinExchange(ctx, "join", coreNonce, body, nil)
}

func (l *Link) renew(ctx context.Context, coreNonce, cert []byte) error {
	id := l.cert.ID
	csr, err := id.CSR()
	if err != nil {
		return err
	}
	return l.joinExchange(ctx, "renew", coreNonce, map[string]any{
		"cert": cert, "csr": csr, "proof": id.Sign(identity.ProofMessage("renew", coreNonce, identity.CSRHash(csr))),
	}, nil)
}

// joinExchange sends a join or a renew and keeps an issued certificate once it
// checks out. errJoinIssued says the certificate was issued; errJoinPending that
// a person must approve the spot.
func (l *Link) joinExchange(ctx context.Context, kind string, coreNonce []byte, body map[string]any, hdr map[string]string) error {
	var out linkIn
	ctxc, cancel := context.WithTimeout(ctx, joinWait)
	defer cancel()
	status, err := l.postH(ctxc, LinkPath, "", hdr, map[string]any{"kind": kind, "coreNonce": coreNonce, "join": body}, &out)
	if err != nil && out.JoinResult == nil {
		return fmt.Errorf("the core's answer to the %s: %w", kind, err)
	}
	if out.Kind != "join_result" || out.JoinResult == nil {
		return fmt.Errorf("the core's answer is %q, not a join_result (%d)", out.Kind, status)
	}
	r := out.JoinResult
	id := l.cert.ID
	switch r.Status {
	case "issued":
		if err := id.ValidateIssued(r.Cert, time.Now()); err != nil {
			return fmt.Errorf("the issued certificate is refused: %w", err)
		}
		if err := id.SetCert(r.Cert); err != nil {
			return err
		}
		l.pendingN = 0
		l.noRenewUntil = time.Now().Add(renewRetry)
		l.log.Info("certificate issued", "spot", l.cert.Name, "fingerprint", id.Fingerprint())
		return errJoinIssued
	case "pending":
		if l.pendingN%10 == 0 {
			l.log.Info("join pending: approve this fingerprint in the core (sources list, or POST /v2/agents/approve)",
				"spot", l.cert.Name, "workspace", l.cert.Workspace, "fingerprint", id.Fingerprint())
		}
		l.pendingN++
		return errJoinPending
	default:
		return fmt.Errorf("the core refused the join: %s", strings.TrimSpace(r.Status+" "+r.Message))
	}
}

// enrollKey is the one-time key in the spot's key file, or "".
func (l *Link) enrollKey() string { return readEnrollKey(&l.cert, l.log) }

// Handler is the actor's HTTP server: the wake-up probe and the core's requests.
func (l *Link) Handler() http.Handler {
	mux := http.NewServeMux()
	// Ready as soon as the server listens: the join and the hello are done inside
	// the first request of the core, which waits for them (a first join waits for a
	// person to approve the spot).
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("POST /frame", l.serveFrame)
	return mux
}

// authorised reports whether the request carries the access token the core got
// from hello: the core puts it on every request it sends the actor. A process
// started for a wake has not said hello yet, so it takes the token its
// predecessor saved, which is the one the core still holds.
func (l *Link) authorised(r *http.Request) bool {
	got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || got == "" {
		return false
	}
	l.mu.Lock()
	known := []string{l.bearer, l.prevBearer}
	l.mu.Unlock()
	if known[0] == "" {
		if b, err := os.ReadFile(l.cert.TokenFile); err == nil {
			known[0] = strings.TrimSpace(string(b))
		}
	}
	match := 0
	for _, k := range known {
		if k != "" {
			match |= subtle.ConstantTimeCompare([]byte(got), []byte(k))
		}
	}
	return match == 1
}

// serveFrame takes one frame of the core. A control is answered in the
// response; a wake, the rooms, the name and the budget are kept for the loop and
// answered 202 at once, the wake report going back when the supervisor is idle.
//
// Anyone who can reach the port can send a request, so a frame counts only with
// the access token. Until the link is up a request without it is a bare poke:
// nothing in it is read, and it starts the join and the hello and asks for one
// plain tick wake (the actor owes the core a wake report). Once the link is up
// a request without the token is refused.
func (l *Link) serveFrame(w http.ResponseWriter, r *http.Request) {
	select {
	case l.frameSlot <- struct{}{}:
		defer func() { <-l.frameSlot }()
	default:
		http.Error(w, "one request at a time", http.StatusTooManyRequests)
		return
	}
	if !l.authorised(r) {
		if l.connected.Load() {
			http.Error(w, "not the core", http.StatusUnauthorized)
			return
		}
		// The first request of a process may come with no token (the core has
		// none to send before the hello): a wake it asked for must not be lost,
		// so the first poke of the process is answered with a plain tick, whose
		// words are the spot's own default and not the request's. One only, so
		// that a stranger cannot make a turn per request.
		l.mu.Lock()
		l.owed = true
		l.mu.Unlock()
		if l.poked.CompareAndSwap(false, true) {
			l.add(frame{Wake: &Order{Reason: "tick"}})
		}
		l.begin()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, "{}\n")
		return
	}
	var f frame
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxFrameBody))
	if err := dec.Decode(&f); err != nil {
		http.Error(w, "not a frame", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if c := f.Control; c != nil && (c.ID == "" || c.Op == "") {
		http.Error(w, "a control needs an id and an op", http.StatusBadRequest)
		return
	}
	l.begin()
	if c := f.Control; c != nil {
		if c.Op == "wake" {
			l.mu.Lock()
			l.owed = true // the core marks the actor running for a wake it asked for
			l.mu.Unlock()
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"controlResult": l.bound(l.run(*c))})
		return
	}
	f.Hello, f.Issues = false, false // the core's frames are the actor's wake; it reads the core itself
	if f.Wake != nil || len(f.Rooms) > 0 {
		l.mu.Lock()
		l.owed = true
		l.mu.Unlock()
	}
	l.add(f)
	w.WriteHeader(http.StatusAccepted)
	_, _ = io.WriteString(w, "{}\n")
}

// bound limits the answer to a control as the stream does.
func (l *Link) bound(res ControlResult) ControlResult {
	if len(res.Tasks) > maxTaskLines {
		res.Tasks = res.Tasks[:maxTaskLines]
	}
	for i := range res.Tasks {
		if t := res.Tasks[i].Title; len(t) > maxTitle {
			res.Tasks[i].Title = strings.ToValidUTF8(t[:maxTitle], "")
		}
	}
	return res
}
