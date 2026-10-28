package identity

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type testCA struct {
	pub  ed25519.PublicKey
	priv ed25519.PrivateKey
	cert *x509.Certificate
	der  []byte
}

func newCA(t *testing.T) *testCA {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "core ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, pub, priv)
	if err != nil {
		t.Fatal(err)
	}
	c, _ := x509.ParseCertificate(der)
	return &testCA{pub, priv, c, der}
}

// issue signs a certificate for pub naming the spot and workspaces, valid from
// notBefore for life.
func (ca *testCA) issue(t *testing.T, pub ed25519.PublicKey, spot string, workspaces []string, notBefore time.Time, life time.Duration) []byte {
	t.Helper()
	uris := []*url.URL{{Scheme: "triage", Opaque: "spot/" + spot}}
	for _, w := range workspaces {
		uris = append(uris, &url.URL{Scheme: "triage", Opaque: "tenant/" + w})
	}
	tpl := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "spot:" + spot}, NotBefore: notBefore, NotAfter: notBefore.Add(life), URIs: uris}
	der, err := x509.CreateCertificate(rand.Reader, tpl, ca.cert, pub, ca.priv)
	if err != nil {
		t.Fatal(err)
	}
	return der
}

func (ca *testCA) challenge(sent []byte, bound bool, ekm []byte) *Challenge {
	nonce := []byte("core-nonce")
	parts := []string{b64(sent)}
	if bound {
		parts = append(parts, b64(ekm))
	}
	return &Challenge{CACert: ca.der, Nonce: nonce, AgentNonce: sent, ChannelBound: bound, Sig: ed25519.Sign(ca.priv, ProofMessage("challenge", nonce, parts...))}
}

func b64(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

func load(t *testing.T, dir, fp string) *Identity {
	t.Helper()
	id, err := Load(dir, "example-spot", "ws-1", fp)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestTheKeyIsGeneratedOnceAndKept(t *testing.T) {
	dir := t.TempDir()
	a := load(t, dir, "")
	b := load(t, dir, "")
	if a.Fingerprint() == "" || a.Fingerprint() != b.Fingerprint() {
		t.Fatalf("fingerprints differ: %q %q", a.Fingerprint(), b.Fingerprint())
	}
	if fi, err := os.Stat(filepath.Join(dir, keyFile)); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("key file: %v %v", fi, err)
	}
}

func TestTheRequestNamesTheSpotAndTheWorkspace(t *testing.T) {
	id := load(t, t.TempDir(), "")
	der, err := id.CSR()
	if err != nil {
		t.Fatal(err)
	}
	csr, err := x509.ParseCertificateRequest(der)
	if err != nil || csr.CheckSignature() != nil {
		t.Fatalf("csr: %v %v", err, csr.CheckSignature())
	}
	spot, ws := names(csr.URIs)
	if csr.Subject.CommonName != "spot:example-spot" || spot != "example-spot" || len(ws) != 1 || ws[0] != "ws-1" {
		t.Fatalf("csr names: %q %q %v", csr.Subject.CommonName, spot, ws)
	}
}

func TestTheChallengeIsCheckedAgainstTheConfiguredFingerprint(t *testing.T) {
	ca := newCA(t)
	sent := []byte("agent-nonce")
	fp := Fingerprint(ca.pub)

	id := load(t, t.TempDir(), fp)
	if err := id.VerifyChallenge(ca.challenge(sent, false, nil), Binding{SentNonce: sent}); err != nil {
		t.Fatalf("good challenge: %v", err)
	}
	if string(id.caCert) != string(ca.der) {
		t.Fatal("the core CA was not saved")
	}

	other := load(t, t.TempDir(), Fingerprint(newCA(t).pub))
	if err := other.VerifyChallenge(ca.challenge(sent, false, nil), Binding{SentNonce: sent}); err == nil {
		t.Fatal("a core CA that is not the configured one was trusted")
	}
	fresh := load(t, t.TempDir(), fp)
	if err := fresh.VerifyChallenge(ca.challenge([]byte("other"), false, nil), Binding{SentNonce: sent}); err == nil {
		t.Fatal("a challenge echoing another nonce was accepted")
	}
	c := ca.challenge(sent, false, nil)
	c.AgentNonce = nil
	if err := fresh.VerifyChallenge(c, Binding{SentNonce: sent}); err == nil {
		t.Fatal("a challenge without the echo was accepted")
	}
	c = ca.challenge(sent, false, nil)
	c.Sig[0] ^= 1
	if err := fresh.VerifyChallenge(c, Binding{SentNonce: sent}); err == nil {
		t.Fatal("a bad signature was accepted")
	}
	if err := fresh.VerifyChallenge(ca.challenge(sent, false, nil), Binding{SentNonce: sent, InsecureTLS: true}); err == nil {
		t.Fatal("an unbound challenge was accepted over unverified TLS")
	}
	ekm := []byte("ekm")
	if err := fresh.VerifyChallenge(ca.challenge(sent, true, ekm), Binding{SentNonce: sent, EKM: ekm}); err != nil {
		t.Fatalf("channel-bound challenge: %v", err)
	}
	if err := fresh.VerifyChallenge(ca.challenge(sent, true, ekm), Binding{SentNonce: sent, EKM: []byte("other")}); err != ErrBoundToOtherChannel {
		t.Fatalf("a challenge bound to another channel: %v", err)
	}
}

func TestAnIssuedCertificateMustBeThisSpotsAndTheWorkspacesOnly(t *testing.T) {
	ca := newCA(t)
	sent := []byte("n")
	id := load(t, t.TempDir(), Fingerprint(ca.pub))
	if err := id.VerifyChallenge(ca.challenge(sent, false, nil), Binding{SentNonce: sent}); err != nil {
		t.Fatal(err)
	}
	pub := id.key.Public().(ed25519.PublicKey)
	now := time.Now()
	good := ca.issue(t, pub, "example-spot", []string{"ws-1"}, now.Add(-time.Minute), time.Hour)
	if err := id.ValidateIssued(good, now); err != nil {
		t.Fatalf("good certificate: %v", err)
	}
	for name, der := range map[string][]byte{
		"another spot":      ca.issue(t, pub, "other-spot", []string{"ws-1"}, now.Add(-time.Minute), time.Hour),
		"another workspace": ca.issue(t, pub, "example-spot", []string{"ws-2"}, now.Add(-time.Minute), time.Hour),
		"two workspaces":    ca.issue(t, pub, "example-spot", []string{"ws-1", "ws-2"}, now.Add(-time.Minute), time.Hour),
		"expired":           ca.issue(t, pub, "example-spot", []string{"ws-1"}, now.Add(-2*time.Hour), time.Hour),
		"another key":       ca.issue(t, newCA(t).pub, "example-spot", []string{"ws-1"}, now.Add(-time.Minute), time.Hour),
		"another CA":        newCA(t).issue(t, pub, "example-spot", []string{"ws-1"}, now.Add(-time.Minute), time.Hour),
	} {
		if err := id.ValidateIssued(der, now); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

func TestACertificateIsDueForRenewalWithAThirdOfItsLifeLeft(t *testing.T) {
	ca := newCA(t)
	id := load(t, t.TempDir(), "")
	pub := id.key.Public().(ed25519.PublicKey)
	now := time.Now()
	if _, usable, _ := id.CertState(now); usable {
		t.Fatal("no certificate is usable")
	}
	if err := id.SetCert(ca.issue(t, pub, "example-spot", []string{"ws-1"}, now.Add(-time.Hour), 3*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, usable, renew := id.CertState(now); !usable || renew {
		t.Fatalf("a young certificate: usable=%v renew=%v", usable, renew)
	}
	if _, usable, renew := id.CertState(now.Add(100 * time.Minute)); !usable || !renew {
		t.Fatalf("an old certificate: usable=%v renew=%v", usable, renew)
	}
	if _, usable, _ := id.CertState(now.Add(3 * time.Hour)); usable {
		t.Fatal("an expired certificate is usable")
	}
	if again := load(t, id.dir, ""); len(again.cert) == 0 {
		t.Fatal("the certificate was not kept")
	}
	id.DropCert()
	if _, usable, _ := id.CertState(now); usable {
		t.Fatal("a dropped certificate is usable")
	}
}

func TestWriteFileReplacesWholeAndIsPrivate(t *testing.T) {
	p := filepath.Join(t.TempDir(), "token")
	for _, v := range []string{"first", "second"} {
		if err := WriteFile(p, []byte(v)); err != nil {
			t.Fatal(err)
		}
		b, _ := os.ReadFile(p)
		if string(b) != v {
			t.Fatalf("got %q", b)
		}
	}
	if fi, _ := os.Stat(p); fi.Mode().Perm() != 0o600 {
		t.Fatalf("mode %v", fi.Mode())
	}
	if m, _ := filepath.Glob(filepath.Join(filepath.Dir(p), ".tmp-*")); len(m) != 0 {
		t.Fatalf("temp files left: %v", m)
	}
}
