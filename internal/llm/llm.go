// Package llm is the only place the spot talks to a model. Client is the
// small interface the agent loop needs; Anthropic implements it over the
// official SDK, and Fake stands in for it in tests.
package llm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// Client runs one model request.
type Client interface {
	// Complete sends the request and returns the assistant's content blocks
	// (text and tool_use), the stop reason and the usage of this one request.
	Complete(ctx context.Context, req Request) (Response, error)
}

// Request is one model request. The conversation is Messages; everything the
// model should see before it (the stable instruction) is System. System and
// Tools form the cacheable prefix of the request, so neither may carry
// anything that changes from wake to wake.
type Request struct {
	System    []string
	Messages  []Message
	Tools     []ToolDef
	MaxTokens int
	// Effort is low, medium, high, xhigh or max; empty = the model's default.
	Effort string
}

// ToolDef is one tool the model may call. InputSchema is the JSON schema of
// its input, passed through as the server gave it.
type ToolDef struct {
	Name        string
	Description string
	InputSchema json.RawMessage
}

// Role is who wrote a message.
type Role string

// Roles.
const (
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
)

// BlockKind says which fields of a Block are used.
type BlockKind string

// Block kinds. Thinking and redacted thinking blocks come back from the model
// and must be sent back unchanged when a tool result is returned to it.
const (
	KindText             BlockKind = "text"
	KindToolUse          BlockKind = "tool_use"
	KindToolResult       BlockKind = "tool_result"
	KindThinking         BlockKind = "thinking"
	KindRedactedThinking BlockKind = "redacted_thinking"
)

// Block is one content block of a message.
type Block struct {
	Kind BlockKind

	// text
	Text string

	// tool_use
	ID    string
	Name  string
	Input json.RawMessage

	// tool_result
	ToolUseID string
	Content   string
	IsError   bool

	// thinking, redacted_thinking: opaque, kept as received
	Thinking  string
	Signature string
	Data      string
}

// Message is one conversation message.
type Message struct {
	Role    Role
	Content []Block
}

// Text is a text block.
func Text(s string) Block { return Block{Kind: KindText, Text: s} }

// ToolResult is a tool_result block answering the tool_use with id.
func ToolResult(id, content string, isError bool) Block {
	return Block{Kind: KindToolResult, ToolUseID: id, Content: content, IsError: isError}
}

// UserText is a user message of one text block.
func UserText(s string) Message { return Message{Role: RoleUser, Content: []Block{Text(s)}} }

// StopReason is why the model stopped.
type StopReason string

// Stop reasons.
const (
	StopEndTurn   StopReason = "end_turn"
	StopToolUse   StopReason = "tool_use"
	StopMaxTokens StopReason = "max_tokens"
	StopRefusal   StopReason = "refusal"
	StopPauseTurn StopReason = "pause_turn"
	StopOther     StopReason = "other"
)

// Usage is the token count of one request or, summed, of a turn. Input is the
// input that was neither read from nor written to the cache.
type Usage struct {
	Input, Output, CacheRead, CacheWrite int64
}

// Add returns u plus o.
func (u Usage) Add(o Usage) Usage {
	return Usage{u.Input + o.Input, u.Output + o.Output, u.CacheRead + o.CacheRead, u.CacheWrite + o.CacheWrite}
}

// Billable is input + output + cache writes. Cache reads are tracked but not
// counted against the daily ceiling: they bill at a fraction of the input rate
// and every step of a turn reads the whole prefix again.
func (u Usage) Billable() int64 { return u.Input + u.Output + u.CacheWrite }

// Context is the size of the conversation at the request: everything the model
// read.
func (u Usage) Context() int64 { return u.Input + u.CacheRead + u.CacheWrite }

// Response is what the model answered.
type Response struct {
	Content    []Block
	StopReason StopReason
	// RefusalCategory is the policy category of a refusal; empty when the
	// model gave none.
	RefusalCategory string
	Usage           Usage
	// Model is the model that answered.
	Model string
}

// APIError is a non-2xx answer from the model API.
type APIError struct {
	Status    int
	Type      string
	Message   string
	RequestID string
}

func (e *APIError) Error() string {
	s := fmt.Sprintf("model API status %d", e.Status)
	if e.Type != "" {
		s += " (" + e.Type + ")"
	}
	if e.Message != "" {
		s += ": " + e.Message
	}
	return s
}

// IsAuth reports whether err is the API refusing the credentials (401 or 403).
func IsAuth(err error) bool {
	var ae *APIError
	return errors.As(err, &ae) && (ae.Status == 401 || ae.Status == 403)
}
