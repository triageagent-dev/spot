package durable

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

func keyFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(p, []byte(content+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

const hexKey = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"

func TestPayloadsAreEncryptedAndComeBack(t *testing.T) {
	dc, _, err := dataConverter(keyFile(t, hexKey))
	if err != nil {
		t.Fatal(err)
	}
	type msg struct{ Text string }
	secret := "the cluster has 12 NotReady nodes"
	ps, err := dc.ToPayloads(msg{Text: secret}, "second")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range ps.Payloads {
		if bytes.Contains(p.Data, []byte(secret)) || bytes.Contains(p.Data, []byte("NotReady")) {
			t.Fatal("the payload holds the text in the clear")
		}
		if string(p.Metadata["encoding"]) != encodingEncrypted {
			t.Fatalf("metadata: %v", p.Metadata)
		}
	}
	var got msg
	var s string
	if err := dc.FromPayloads(ps, &got, &s); err != nil {
		t.Fatal(err)
	}
	if got.Text != secret || s != "second" {
		t.Fatalf("round trip: %+v %q", got, s)
	}
}

func TestAnotherKeyCannotReadAPayload(t *testing.T) {
	a, _, _ := dataConverter(keyFile(t, hexKey))
	b, _, err := dataConverter(keyFile(t, strings.Repeat("ab", 32)))
	if err != nil {
		t.Fatal(err)
	}
	ps, _ := a.ToPayloads("x")
	var s string
	if err := b.FromPayloads(ps, &s); err == nil || !strings.Contains(err.Error(), "key") {
		t.Fatalf("another key read it: %v", err)
	}
}

func TestAPlainPayloadStillReads(t *testing.T) {
	dc, _, _ := dataConverter(keyFile(t, hexKey))
	ps, _ := converter.GetDefaultDataConverter().ToPayloads("old")
	var s string
	if err := dc.FromPayloads(ps, &s); err != nil || s != "old" {
		t.Fatalf("%q %v", s, err)
	}
}

func TestAKeyOfTheWrongSizeIsRefused(t *testing.T) {
	for _, k := range []string{"", "abcd", strings.Repeat("zz", 32)} {
		if _, _, err := dataConverter(keyFile(t, k)); err == nil {
			t.Errorf("key %q was accepted", k)
		}
	}
	if _, _, err := dataConverter(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Error("a missing file was accepted")
	}
}

func TestClaimedPayloadsLeaveNothingInThePayloadAndComeBack(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "payloads")
	dc, fc, err := converters("", dir)
	if err != nil || fc == nil {
		t.Fatalf("converters: %v %v", err, fc)
	}
	type msg struct{ Text string }
	secret := "the cluster has 12 NotReady nodes"
	ps, err := dc.ToPayloads(msg{Text: secret}, "second")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range ps.Payloads {
		if bytes.Contains(p.Data, []byte("NotReady")) || bytes.Contains(p.Data, []byte(secret)) {
			t.Fatal("the payload sent to the service holds the text")
		}
		if string(p.Metadata["encoding"]) != encodingClaim || len(p.Data) != 64 {
			t.Fatalf("not a reference: %v %q", p.Metadata, p.Data)
		}
	}
	var got msg
	var s string
	if err := dc.FromPayloads(ps, &got, &s); err != nil {
		t.Fatal(err)
	}
	if got.Text != secret || s != "second" {
		t.Fatalf("round trip: %+v %q", got, s)
	}
	// The same payload is stored once; the files are private to the pod.
	if _, err := dc.ToPayloads(msg{Text: secret}, "second"); err != nil {
		t.Fatal(err)
	}
	ents, _ := os.ReadDir(dir)
	if len(ents) != 2 {
		t.Fatalf("%d files, want one per distinct payload (2)", len(ents))
	}
	for _, e := range ents {
		if info, _ := e.Info(); info.Mode().Perm() != 0o600 {
			t.Errorf("%s mode %v", e.Name(), info.Mode())
		}
	}
}

func TestAClaimThatIsGoneFailsClearly(t *testing.T) {
	dir := t.TempDir()
	dc, _, err := converters("", dir)
	if err != nil {
		t.Fatal(err)
	}
	ps, _ := dc.ToPayloads("x")
	ents, _ := os.ReadDir(dir)
	for _, e := range ents {
		_ = os.Remove(filepath.Join(dir, e.Name()))
	}
	var s string
	if err := dc.FromPayloads(ps, &s); err == nil || !strings.Contains(err.Error(), "not in this pod's store") {
		t.Fatalf("err = %v", err)
	}
}

func TestAClaimCannotNameAFileOutsideTheStore(t *testing.T) {
	c, err := newClaimCodec(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	bad := &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte(encodingClaim)}, Data: []byte("../../etc/passwd")}
	if _, err := c.Decode([]*commonpb.Payload{bad}); err == nil {
		t.Fatal("a path was taken for a payload id")
	}
}

func TestSweepRemovesOnlyThePayloadsNobodyReads(t *testing.T) {
	dir := t.TempDir()
	c, _ := newClaimCodec(dir)
	dc := converter.NewCodecDataConverter(converter.GetDefaultDataConverter(), c)
	old, _ := dc.ToPayloads("old one")
	kept, _ := dc.ToPayloads("kept one")
	age := time.Now().Add(-30 * 24 * time.Hour)
	for _, ps := range []*commonpb.Payloads{old, kept} {
		_ = os.Chtimes(c.path(string(ps.Payloads[0].Data)), age, age)
	}
	var s string
	if err := dc.FromPayloads(kept, &s); err != nil { // a read keeps it
		t.Fatal(err)
	}
	if n := c.sweep(21 * 24 * time.Hour); n != 1 {
		t.Fatalf("swept %d, want 1", n)
	}
	if _, err := os.Stat(c.path(string(kept.Payloads[0].Data))); err != nil {
		t.Errorf("the payload that was read is gone: %v", err)
	}
	if _, err := os.Stat(c.path(string(old.Payloads[0].Data))); err == nil {
		t.Error("the old payload is still there")
	}
}
