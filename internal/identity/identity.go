// Package identity is the spot's certificate identity (the core's ADR-105): an
// Ed25519 key on the spot's state volume, a certificate request naming the spot
// and its workspace, the check of the core's challenge against a configured CA
// fingerprint, the check of the certificate the core issues, and the file the
// short-lived access token is written to.
//
// The request layout, proof message and challenge check mirror the agent's
// push/join.go and the core's internal/agentjoin byte for byte; the repos are
// separate Go modules. Nothing here talks to the core: the join frames are the
// supervisor's trigger stream's.
package identity

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

// The files of the identity, in the state directory.
const (
	keyFile  = "key.pem"
	certFile = "cert.der"
	caFile   = "ca.der"
)

// Identity is the spot's key, its current certificate and the core CA it trusts,
// kept in one directory (mode 0700, files 0600).
type Identity struct {
	mu            sync.Mutex
	dir           string
	spot          string // the spot's name
	workspace     string // the workspace it works for
	caFingerprint string // the configured trust anchor
	key           ed25519.PrivateKey
	cert          []byte
	caCert        []byte // saved from the first verified challenge
}

// Load reads the identity from dir, generating and saving a key on first use.
// caFingerprint is the SHA-256 form the core's CA is configured by.
func Load(dir, spot, workspace, caFingerprint string) (*Identity, error) {
	if spot == "" || workspace == "" {
		return nil, errors.New("identity: a spot name and a workspace are required")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("identity: %w", err)
	}
	id := &Identity{dir: dir, spot: spot, workspace: workspace, caFingerprint: caFingerprint}
	b, err := id.read(keyFile)
	if err != nil {
		return nil, err
	}
	if len(b) > 0 {
		blk, _ := pem.Decode(b)
		if blk == nil {
			return nil, errors.New("identity: key.pem has no PEM block")
		}
		k, err := x509.ParsePKCS8PrivateKey(blk.Bytes)
		if err != nil {
			return nil, fmt.Errorf("identity: key: %w", err)
		}
		key, ok := k.(ed25519.PrivateKey)
		if !ok {
			return nil, errors.New("identity: key is not an Ed25519 key")
		}
		id.key = key
	} else {
		_, key, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return nil, err
		}
		der, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			return nil, err
		}
		if err := WriteFile(filepath.Join(dir, keyFile), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})); err != nil {
			return nil, fmt.Errorf("identity: save key: %w", err)
		}
		id.key = key
	}
	if id.cert, err = id.read(certFile); err != nil {
		return nil, err
	}
	if id.caCert, err = id.read(caFile); err != nil {
		return nil, err
	}
	return id, nil
}

func (id *Identity) read(name string) ([]byte, error) {
	b, err := os.ReadFile(filepath.Join(id.dir, name))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	return b, err
}

// Fingerprint is "SHA256:<hex>" of the key's PKIX encoding: what the spot logs
// while it waits for approval and what the person approving compares.
func (id *Identity) Fingerprint() string { return Fingerprint(id.key.Public().(ed25519.PublicKey)) }

// Sign signs msg with the spot's key.
func (id *Identity) Sign(msg []byte) []byte { return ed25519.Sign(id.key, msg) }

// CSR is the certificate request for the spot: CN spot:<name>, and the URIs
// triage:spot/<name> and triage:tenant/<workspace>.
func (id *Identity) CSR() ([]byte, error) {
	return x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: "spot:" + id.spot},
		URIs: []*url.URL{
			{Scheme: "triage", Opaque: "spot/" + id.spot},
			{Scheme: "triage", Opaque: "tenant/" + id.workspace},
		},
	}, id.key)
}

// CSRHash is the hash the proof of a join names its request by.
func CSRHash(csr []byte) string {
	sum := sha256.Sum256(csr)
	return hex.EncodeToString(sum[:])
}

// Challenge is what the core sends first, to prove itself. The agent's names.
type Challenge struct {
	CACert       []byte
	CrossCert    []byte
	Nonce        []byte
	AgentNonce   []byte // the nonce this spot sent on the dial, echoed back
	Sig          []byte
	ChannelBound bool
}

// Binding is what the spot knows of its own connection.
type Binding struct {
	SentNonce   []byte // the nonce sent on this dial
	EKM         []byte // TLS exporter keying material, nil when not TLS
	InsecureTLS bool   // the core's TLS name is not verified
}

// ErrBoundToOtherChannel marks a challenge bound to another TLS channel.
var ErrBoundToOtherChannel = errors.New("challenge is bound to a different TLS channel")

// VerifyChallenge checks the core's challenge and saves the CA certificate it
// trusts from then on.
func (id *Identity) VerifyChallenge(c *Challenge, b Binding) error {
	id.mu.Lock()
	defer id.mu.Unlock()
	trusted, err := verifyChallenge(id.caFingerprint, id.caCert, c, b, time.Now())
	if err != nil {
		return err
	}
	if !bytes.Equal(trusted, id.caCert) {
		if err := WriteFile(filepath.Join(id.dir, caFile), trusted); err != nil {
			return fmt.Errorf("save core CA: %w", err)
		}
		id.caCert = trusted
	}
	return nil
}

// ValidateIssued checks a certificate the core returned before it is saved: it
// chains to the trusted CA, is valid now, carries this spot's key, and names
// exactly this spot and workspace.
func (id *Identity) ValidateIssued(der []byte, now time.Time) error {
	id.mu.Lock()
	defer id.mu.Unlock()
	return validateIssued(der, id.caCert, id.key.Public().(ed25519.PublicKey), now, id.spot, id.workspace)
}

// SetCert saves an issued certificate.
func (id *Identity) SetCert(der []byte) error {
	id.mu.Lock()
	defer id.mu.Unlock()
	if err := WriteFile(filepath.Join(id.dir, certFile), der); err != nil {
		return fmt.Errorf("save certificate: %w", err)
	}
	id.cert = der
	return nil
}

// DropCert forgets the certificate, so the spot asks for a new one.
func (id *Identity) DropCert() {
	id.mu.Lock()
	defer id.mu.Unlock()
	_ = os.Remove(filepath.Join(id.dir, certFile))
	id.cert = nil
}

// CertState says whether the saved certificate can open a session for this spot
// and workspace, and whether it is due for renewal (a third of its life left).
func (id *Identity) CertState(now time.Time) (cert []byte, usable, renew bool) {
	id.mu.Lock()
	defer id.mu.Unlock()
	if len(id.cert) == 0 {
		return nil, false, false
	}
	c, err := x509.ParseCertificate(id.cert)
	if err != nil || !now.Before(c.NotAfter) {
		return nil, false, false
	}
	pub, ok := c.PublicKey.(ed25519.PublicKey)
	if !ok || !pub.Equal(id.key.Public()) {
		return nil, false, false
	}
	spot, tenants := names(c.URIs)
	if spot != id.spot || !slices.Equal(tenants, []string{id.workspace}) {
		return nil, false, false
	}
	life := c.NotAfter.Sub(c.NotBefore)
	return id.cert, true, c.NotAfter.Sub(now) < life/3
}

func validateIssued(der, caDER []byte, pub ed25519.PublicKey, now time.Time, spot, workspace string) error {
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return fmt.Errorf("issued certificate does not parse: %w", err)
	}
	if len(caDER) == 0 {
		return errors.New("issued certificate cannot be checked: no trusted CA certificate")
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		return fmt.Errorf("parse trusted CA: %w", err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	if _, err := cert.Verify(x509.VerifyOptions{Roots: roots, CurrentTime: now, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}}); err != nil {
		return fmt.Errorf("issued certificate does not chain to the trusted CA or is not currently valid: %w", err)
	}
	if k, ok := cert.PublicKey.(ed25519.PublicKey); !ok || !k.Equal(pub) {
		return errors.New("issued certificate does not carry this spot's key")
	}
	gotSpot, gotTenants := names(cert.URIs)
	if gotSpot != spot {
		return fmt.Errorf("issued certificate names spot %q, want triage:spot/%s", gotSpot, spot)
	}
	if !slices.Equal(gotTenants, []string{workspace}) {
		return fmt.Errorf("issued certificate names workspaces %v, want exactly [%s]", gotTenants, workspace)
	}
	return nil
}

func names(uris []*url.URL) (spot string, tenants []string) {
	for _, u := range uris {
		if u.Scheme != "triage" {
			continue
		}
		if s, ok := strings.CutPrefix(u.Opaque, "spot/"); ok {
			spot = s
		} else if t, ok := strings.CutPrefix(u.Opaque, "tenant/"); ok {
			tenants = append(tenants, t)
		}
	}
	sort.Strings(tenants)
	return spot, tenants
}

// Fingerprint mirrors agentjoin.Fingerprint.
func Fingerprint(pub ed25519.PublicKey) string {
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(der)
	return "SHA256:" + hex.EncodeToString(sum[:])
}

// ProofMessage mirrors agentjoin.ProofMessage.
func ProofMessage(kind string, nonce []byte, parts ...string) []byte {
	var b bytes.Buffer
	b.WriteString("triage-agent/" + kind + "\n" + base64.StdEncoding.EncodeToString(nonce))
	for _, p := range parts {
		b.WriteString("\n" + p)
	}
	return b.Bytes()
}

// verifyChallenge mirrors the agent's verifyJoinChallenge: the core CA must be
// the trusted one (the saved certificate, else the configured fingerprint), or
// be certified by the saved certificate through the cross certificate during a
// rotation; its signature must cover the challenge message, which echoes the
// nonce this spot sent (a challenge without the echo is replayable and refused)
// and, when the core binds the TLS channel, the exporter keying material.
func verifyChallenge(trustedFP string, trustedDER []byte, c *Challenge, b Binding, now time.Time) ([]byte, error) {
	caCert, err := x509.ParseCertificate(c.CACert)
	if err != nil {
		return nil, fmt.Errorf("parse core CA: %w", err)
	}
	pub, ok := caCert.PublicKey.(ed25519.PublicKey)
	if !ok {
		return nil, errors.New("core CA key must be Ed25519")
	}
	var trusted *x509.Certificate
	if len(trustedDER) > 0 {
		if trusted, err = x509.ParseCertificate(trustedDER); err != nil {
			return nil, fmt.Errorf("parse trusted CA: %w", err)
		}
		tp, ok := trusted.PublicKey.(ed25519.PublicKey)
		if !ok {
			return nil, errors.New("trusted CA key must be Ed25519")
		}
		trustedFP = Fingerprint(tp)
	}
	if Fingerprint(pub) != trustedFP {
		if trusted == nil || len(c.CrossCert) == 0 {
			return nil, fmt.Errorf("core CA %s is not the trusted CA %s", Fingerprint(pub), trustedFP)
		}
		cross, err := x509.ParseCertificate(c.CrossCert)
		if err != nil {
			return nil, fmt.Errorf("parse cross certificate: %w", err)
		}
		if cp, ok := cross.PublicKey.(ed25519.PublicKey); !ok || !cp.Equal(pub) {
			return nil, errors.New("cross certificate does not certify the core CA")
		}
		if err := cross.CheckSignatureFrom(trusted); err != nil {
			return nil, fmt.Errorf("cross certificate not signed by the trusted CA: %w", err)
		}
		if !cross.IsCA || !cross.BasicConstraintsValid || cross.KeyUsage&x509.KeyUsageCertSign == 0 {
			return nil, errors.New("cross certificate is not a certificate-signing CA certificate")
		}
		if now.Before(cross.NotBefore) || !now.Before(cross.NotAfter) {
			return nil, errors.New("cross certificate is not currently valid")
		}
	}
	if len(c.AgentNonce) == 0 {
		return nil, errors.New("core does not bind the challenge to this connection")
	}
	if !bytes.Equal(c.AgentNonce, b.SentNonce) {
		return nil, errors.New("challenge echoes a different nonce")
	}
	parts := []string{base64.StdEncoding.EncodeToString(c.AgentNonce)}
	switch {
	case c.ChannelBound:
		if len(b.EKM) == 0 {
			return nil, ErrBoundToOtherChannel
		}
		parts = append(parts, base64.StdEncoding.EncodeToString(b.EKM))
	case b.InsecureTLS:
		return nil, errors.New("core did not bind the challenge to the TLS channel and its TLS name is not verified")
	}
	if !ed25519.Verify(pub, ProofMessage("challenge", c.Nonce, parts...), c.Sig) {
		if c.ChannelBound {
			return nil, ErrBoundToOtherChannel
		}
		return nil, errors.New("challenge signature does not verify")
	}
	return c.CACert, nil
}

// WriteFile writes data to path through a rename, mode 0600, so a reader (the
// MCP client reads the token file on every request) never sees half a file.
func WriteFile(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer func() { _ = os.Remove(name) }()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}
