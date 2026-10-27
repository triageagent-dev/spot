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
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
)

const (
	encodingEncrypted = "binary/encrypted"
	keyIDMetadata     = "encryption-key-id"
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

// dataConverter is the default converter, with the codec when there is a key
// file. Failures (their messages and stack traces) are encrypted too.
func dataConverter(keyFile string) (converter.DataConverter, converter.FailureConverter, error) {
	dc := converter.GetDefaultDataConverter()
	if keyFile == "" {
		return dc, nil, nil
	}
	c, err := newCodec(keyFile)
	if err != nil {
		return nil, nil, err
	}
	dc = converter.NewCodecDataConverter(dc, c)
	fc := temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{
		DataConverter: dc, EncodeCommonAttributes: true,
	})
	return dc, fc, nil
}
