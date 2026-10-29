package triggers

// The certificate identity of the stream (the core's ADR-105). The spot dials the
// core's join route with a nonce, checks the core's challenge against the CA
// fingerprint it was given, then does one of three things, by what it holds:
// asks for a certificate (join), asks for a new one before the old one ends
// (renew), or opens the stream with the certificate (hello). After a good hello
// the socket is the trigger stream of Path, and the core also sends a short-lived
// access token, which is written to the file the MCP client reads.

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"github.com/triageagent-dev/spot/internal/identity"
)

// JoinPath is the core's route for a spot that joins by certificate.
const JoinPath = "/v2/messenger/spot/join"

// Cert is how the spot proves who it is: its identity, and where the token the
// core hands it is written.
type Cert struct {
	ID        *identity.Identity
	Name      string // the spot's name, as in its certificate
	Workspace string
	Version   string
	TokenFile string
	// EnrollKeyFile holds the one-time key that makes the core approve the join
	// at once; empty = none. It is sent only while the spot has no certificate.
	EnrollKeyFile string
}

// joinWait is how long the core may take to answer a join or a renew, and the
// longest it waits for the challenge.
const joinWait = 30 * time.Second

// renewRetry is how long the spot opens the stream with the certificate it has,
// after it was issued or a renewal failed, before it asks for a new one: a core
// that issues short certificates cannot keep the spot renewing in a loop.
const renewRetry = 5 * time.Minute

// ekmLabel is the TLS exporter label the core binds its challenge with.
const ekmLabel = "EXPORTER-triage-agent-join"

type tokenFrame struct {
	Value string `json:"value"`
	Exp   int64  `json:"exp"`
}

type wireChallenge struct {
	Nonce        []byte `json:"nonce"`
	CACert       []byte `json:"caCert"`
	CrossCert    []byte `json:"crossCert,omitempty"`
	Sig          []byte `json:"sig"`
	AgentNonce   []byte `json:"agentNonce"`
	ChannelBound bool   `json:"channelBound,omitempty"`
}

type wireResult struct {
	Status  string `json:"status"` // pending, issued or refused
	Cert    []byte `json:"cert,omitempty"`
	CACert  []byte `json:"caCert,omitempty"`
	Message string `json:"message,omitempty"`
}

type joinIn struct {
	Kind       string         `json:"kind"`
	Challenge  *wireChallenge `json:"challenge,omitempty"`
	JoinResult *wireResult    `json:"joinResult,omitempty"`
}

// NewCert builds a client that joins the core by certificate. apiURL is the
// core's HTTP API base URL, as for New.
func NewCert(apiURL string, cert Cert, log *slog.Logger) (*Client, error) {
	if cert.ID == nil || cert.Name == "" || cert.Workspace == "" || cert.TokenFile == "" {
		return nil, errors.New("a certificate identity needs an identity, a name, a workspace and a token file")
	}
	c, err := New(apiURL, cert.Workspace, cert.TokenFile, log)
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(c.url)
	if err != nil {
		return nil, err
	}
	u.Path = strings.TrimSuffix(u.Path, Path) + JoinPath
	u.RawQuery = ""
	c.url = u.String()
	c.cert = &cert
	c.ready = make(chan struct{})
	return c, nil
}

// WaitReady blocks until the stream has given the spot its first access token,
// so that what the spot reads from the core works. It returns at once for a
// client that does not join by certificate.
func (c *Client) WaitReady(ctx context.Context) error {
	if c.cert == nil {
		return nil
	}
	select {
	case <-c.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

var errJoinIssued = errors.New("certificate issued; reconnecting with it")
var errJoinPending = errors.New("join request pending approval")

// onceCert is once for a certificate identity.
func (c *Client) onceCert(ctx context.Context) (bool, error) {
	nonce := make([]byte, 32)
	if _, err := rand.Read(nonce); err != nil {
		return false, err
	}
	conn, err := c.dial(ctx, http.Header{"X-Triage-Agent-Nonce": {base64.StdEncoding.EncodeToString(nonce)}})
	if err != nil {
		return false, err
	}
	hello, up, err := c.handshake(ctx, conn, nonce)
	if err != nil || !hello {
		_ = conn.Close()
		return up, err
	}
	return c.serve(ctx, conn)
}

// handshake answers the core's challenge and does what the spot's certificate
// calls for. hello is true when the stream is open for serving.
func (c *Client) handshake(ctx context.Context, conn *websocket.Conn, nonce []byte) (hello, up bool, err error) {
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	_ = conn.SetReadDeadline(time.Now().Add(joinWait))
	var f joinIn
	if err := conn.ReadJSON(&f); err != nil {
		return false, false, fmt.Errorf("reading the core's challenge: %w", err)
	}
	if f.Kind != "challenge" || f.Challenge == nil {
		return false, false, fmt.Errorf("the core's first frame is %q, not a challenge", f.Kind)
	}
	ch := f.Challenge
	bind := identity.Binding{SentNonce: nonce}
	if ch.ChannelBound {
		if tc, ok := conn.UnderlyingConn().(*tls.Conn); ok {
			st := tc.ConnectionState()
			bind.EKM, _ = st.ExportKeyingMaterial(ekmLabel, nil, 32)
		}
	}
	id := c.cert.ID
	if err := id.VerifyChallenge(&identity.Challenge{
		CACert: ch.CACert, CrossCert: ch.CrossCert, Nonce: ch.Nonce, AgentNonce: ch.AgentNonce, Sig: ch.Sig, ChannelBound: ch.ChannelBound,
	}, bind); err != nil {
		return false, false, fmt.Errorf("the core is not the one this spot trusts: %w", err)
	}

	cert, usable, renew := id.CertState(time.Now())
	switch {
	case !usable:
		up, err := c.join(conn, ch.Nonce)
		return false, up, err
	case renew && time.Now().After(c.noRenewUntil):
		up, err := c.renew(conn, ch.Nonce, cert)
		if err != nil && !errors.Is(err, errJoinIssued) {
			// The core closes after its answer. The certificate still opens a
			// stream, so the next connection says hello and tries again later.
			c.noRenewUntil = time.Now().Add(renewRetry)
			c.log.Warn("certificate renewal failed; the current certificate is used until the next try", "err", err, "retryIn", renewRetry.String())
		}
		return false, up, err
	}
	hello_ := map[string]any{"kind": "hello", "hello": map[string]any{
		"spot": c.cert.Name, "workspace": c.cert.Workspace, "version": c.cert.Version, "cert": cert,
		"proof": id.Sign(identity.ProofMessage("hello", ch.Nonce, c.cert.Name, c.cert.Workspace)),
	}}
	if err := conn.WriteJSON(hello_); err != nil {
		return false, false, err
	}
	_ = conn.SetReadDeadline(time.Now().Add(readIdle))
	return true, true, nil
}

// join asks for a certificate. A pending request is retried by the caller's
// backoff until a person approves it.
func (c *Client) join(conn *websocket.Conn, nonce []byte) (bool, error) {
	id := c.cert.ID
	csr, err := id.CSR()
	if err != nil {
		return false, err
	}
	body := map[string]any{"csr": csr, "proof": id.Sign(identity.ProofMessage("join", nonce, identity.CSRHash(csr)))}
	if k := c.enrollKey(); k != "" {
		body["enrollKey"] = k
	}
	if err := conn.WriteJSON(map[string]any{"kind": "join", "join": body}); err != nil {
		return false, err
	}
	return c.result(conn)
}

// enrollKey is the one-time key in the spot's key file, or "".
func (c *Client) enrollKey() string { return readEnrollKey(c.cert, c.log) }

func readEnrollKey(cert *Cert, log *slog.Logger) string {
	if cert.EnrollKeyFile == "" {
		return ""
	}
	b, err := os.ReadFile(cert.EnrollKeyFile)
	if err != nil {
		log.Warn("the enrollment key cannot be read; joining without it", "err", err)
		return ""
	}
	return strings.TrimSpace(string(b))
}

// renew asks for a new certificate with a request signed by the current key.
func (c *Client) renew(conn *websocket.Conn, nonce, cert []byte) (bool, error) {
	id := c.cert.ID
	csr, err := id.CSR()
	if err != nil {
		return false, err
	}
	if err := conn.WriteJSON(map[string]any{"kind": "renew", "join": map[string]any{
		"cert": cert, "csr": csr, "proof": id.Sign(identity.ProofMessage("renew", nonce, identity.CSRHash(csr))),
	}}); err != nil {
		return false, err
	}
	return c.result(conn)
}

// result reads the core's answer to a join or a renew and keeps an issued
// certificate once it checks out.
func (c *Client) result(conn *websocket.Conn) (bool, error) {
	_ = conn.SetReadDeadline(time.Now().Add(joinWait))
	var f joinIn
	if err := conn.ReadJSON(&f); err != nil {
		return false, fmt.Errorf("reading the core's answer: %w", err)
	}
	if f.Kind != "join_result" || f.JoinResult == nil {
		return false, fmt.Errorf("the core's answer is %q, not a join_result", f.Kind)
	}
	r := f.JoinResult
	id := c.cert.ID
	switch r.Status {
	case "issued":
		if err := id.ValidateIssued(r.Cert, time.Now()); err != nil {
			return false, fmt.Errorf("the issued certificate is refused: %w", err)
		}
		if err := id.SetCert(r.Cert); err != nil {
			return false, err
		}
		c.pendingN = 0
		c.noRenewUntil = time.Now().Add(renewRetry)
		c.log.Info("certificate issued", "spot", c.cert.Name, "fingerprint", id.Fingerprint())
		return true, errJoinIssued
	case "pending":
		if c.pendingN%10 == 0 {
			c.log.Info("join pending: approve this fingerprint in the core (sources list, or POST /v2/agents/approve)",
				"spot", c.cert.Name, "workspace", c.cert.Workspace, "fingerprint", id.Fingerprint())
		}
		c.pendingN++
		return false, errJoinPending
	default:
		return false, fmt.Errorf("the core refused the join: %s", strings.TrimSpace(r.Status+" "+r.Message))
	}
}

// takeToken writes the access token the core sent to its file, and lets
// WaitReady go.
func (c *Client) takeToken(t *tokenFrame) {
	if c.cert == nil || t.Value == "" {
		return
	}
	if err := identity.WriteFile(c.cert.TokenFile, []byte(t.Value)); err != nil {
		c.log.Error("the core's access token could not be saved", "err", err)
		return
	}
	c.readyOnce.Do(func() { close(c.ready) })
}

// afterClose reads why an open stream ended. A policy close (1008) is the core
// not accepting the certificate (revoked, expired, mismatched): the spot forgets
// it and joins afresh. A 1013 means the core cannot say yet; the certificate is
// kept.
func (c *Client) afterClose(err error) {
	var ce *websocket.CloseError
	if c.cert == nil || !errors.As(err, &ce) {
		return
	}
	if ce.Code == websocket.ClosePolicyViolation {
		c.log.Warn("the core does not accept the certificate; the spot will join afresh", "reason", ce.Text)
		c.cert.ID.DropCert()
	}
}
