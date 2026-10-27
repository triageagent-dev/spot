package durable

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
