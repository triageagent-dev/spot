package durable

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
)

const (
	encodingEncrypted = "binary/encrypted"
	keyIDMetadata     = "encryption-key-id"
	encodingClaim     = "binary/claim"
)

// codec encrypts every payload with AES-256-GCM before it leaves the process, so
// the Temporal service (a cloud one above all) holds only ciphertext: the wake
// prompt, the model's words and the tool results never reach it in the clear.
type codec struct {
	aead cipher.AEAD
	id   string
}

// newCodec reads the key from a file: 32 bytes as 64 hex characters or as
// base64. The key is a file, never an argument or a variable.
func newCodec(keyFile string) (*codec, error) {
	raw, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, fmt.Errorf("payload key file: %w", err)
	}
	key, err := parseKey(strings.TrimSpace(string(raw)))
	if err != nil {
		return nil, fmt.Errorf("payload key file: %w", err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(key)
	return &codec{aead: aead, id: hex.EncodeToString(sum[:4])}, nil
}

func parseKey(s string) ([]byte, error) {
	if b, err := hex.DecodeString(s); err == nil && len(b) == 32 {
		return b, nil
	}
	if b, err := base64.StdEncoding.DecodeString(s); err == nil && len(b) == 32 {
		return b, nil
	}
	return nil, errors.New("the key must be 32 bytes, as 64 hex characters or as base64")
}

// Encode implements converter.PayloadCodec.
func (c *codec) Encode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	out := make([]*commonpb.Payload, len(payloads))
	for i, p := range payloads {
		plain, err := p.Marshal()
		if err != nil {
			return nil, err
		}
		nonce := make([]byte, c.aead.NonceSize())
		if _, err := rand.Read(nonce); err != nil {
			return nil, err
		}
		out[i] = &commonpb.Payload{
			Metadata: map[string][]byte{"encoding": []byte(encodingEncrypted), keyIDMetadata: []byte(c.id)},
			Data:     c.aead.Seal(nonce, nonce, plain, nil),
		}
	}
	return out, nil
}

// Decode implements converter.PayloadCodec. A payload that was not encrypted
// passes through, so a workflow started before the key was set still reads.
func (c *codec) Decode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	out := make([]*commonpb.Payload, len(payloads))
	for i, p := range payloads {
		if string(p.GetMetadata()["encoding"]) != encodingEncrypted {
			out[i] = p
			continue
		}
		if id := string(p.Metadata[keyIDMetadata]); id != c.id {
			return nil, fmt.Errorf("a payload was encrypted with key %s, this process holds key %s", id, c.id)
		}
		n := c.aead.NonceSize()
		if len(p.Data) < n {
			return nil, errors.New("an encrypted payload is too short")
		}
		plain, err := c.aead.Open(nil, p.Data[:n], p.Data[n:], nil)
		if err != nil {
			return nil, fmt.Errorf("decrypting a payload: %w", err)
		}
		var dec commonpb.Payload
		if err := dec.Unmarshal(plain); err != nil {
			return nil, err
		}
		out[i] = &dec
	}
	return out, nil
}

// claimCodec keeps every payload in a directory of the pod and sends the
// service a reference to it: the service's history holds the sha-256 of the
// payload and nothing else, so no content (not even as ciphertext) leaves the
// process. The workflow replays from its history by reading the payloads back
// here, so the directory must outlive the workflows that use it: a payload that
// is gone fails the replay, and the operator ends that workflow.
type claimCodec struct {
	dir string
}

func newClaimCodec(dir string) (*claimCodec, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("payload directory: %w", err)
	}
	return &claimCodec{dir: dir}, nil
}

func (c *claimCodec) path(id string) string { return filepath.Join(c.dir, id) }

// validID reports whether id is the hex sha-256 a claim holds: nothing else may
// name a file here.
func validID(id string) bool {
	if len(id) != 64 {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil
}

// Encode implements converter.PayloadCodec. The same payload is stored once.
func (c *claimCodec) Encode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	out := make([]*commonpb.Payload, len(payloads))
	for i, p := range payloads {
		plain, err := p.Marshal()
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256(plain)
		id := hex.EncodeToString(sum[:])
		if _, err := os.Stat(c.path(id)); err != nil {
			if err := writeAtomic(c.path(id), plain); err != nil {
				return nil, fmt.Errorf("storing a payload: %w", err)
			}
		}
		out[i] = &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte(encodingClaim)}, Data: []byte(id)}
	}
	return out, nil
}

// Decode implements converter.PayloadCodec. A payload that is not a claim
// passes through, so a workflow started before the mode was set still reads.
func (c *claimCodec) Decode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	out := make([]*commonpb.Payload, len(payloads))
	for i, p := range payloads {
		if string(p.GetMetadata()["encoding"]) != encodingClaim {
			out[i] = p
			continue
		}
		id := string(p.Data)
		if !validID(id) {
			return nil, errors.New("a payload reference is not a payload id")
		}
		plain, err := os.ReadFile(c.path(id))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("payload %.12s is not in this pod's store: it was swept or this is another pod's state", id)
			}
			return nil, err
		}
		now := time.Now()
		_ = os.Chtimes(c.path(id), now, now) // a payload that is read is kept
		var dec commonpb.Payload
		if err := dec.Unmarshal(plain); err != nil {
			return nil, err
		}
		out[i] = &dec
	}
	return out, nil
}

// sweep removes the payloads nobody wrote or read for ttl and says how many.
func (c *claimCodec) sweep(ttl time.Duration) int {
	ents, err := os.ReadDir(c.dir)
	if err != nil {
		return 0
	}
	n := 0
	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil || time.Since(info.ModTime()) < ttl {
			continue
		}
		if os.Remove(filepath.Join(c.dir, e.Name())) == nil {
			n++
		}
	}
	return n
}

// writeAtomic writes the file whole or not at all: a payload half written
// would be read back as the real one.
func writeAtomic(path string, b []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-")
	if err != nil {
		return err
	}
	name := tmp.Name()
	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return err
	}
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(name)
		return err
	}
	return os.Rename(name, path)
}

// dataConverter is the default converter, with the codec when there is a key
// file. Failures (their messages and stack traces) are encrypted too.
func dataConverter(keyFile string) (converter.DataConverter, converter.FailureConverter, error) {
	return converters(keyFile, "")
}

// converters is the converter of a process: with a payload directory every
// payload is kept there and only a reference is sent; with a key file every
// payload is encrypted; with neither they are sent as they are. The directory
// wins over the key.
func converters(keyFile, payloadDir string) (converter.DataConverter, converter.FailureConverter, error) {
	dc := converter.GetDefaultDataConverter()
	var pc converter.PayloadCodec
	switch {
	case payloadDir != "":
		c, err := newClaimCodec(payloadDir)
		if err != nil {
			return nil, nil, err
		}
		pc = c
	case keyFile != "":
		c, err := newCodec(keyFile)
		if err != nil {
			return nil, nil, err
		}
		pc = c
	default:
		return dc, nil, nil
	}
	dc = converter.NewCodecDataConverter(dc, pc)
	fc := temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{
		DataConverter: dc, EncodeCommonAttributes: true,
	})
	return dc, fc, nil
}
