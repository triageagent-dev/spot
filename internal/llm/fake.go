package llm

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
)

// Step is one scripted answer of a Fake: a Response, an error, or a request
// that never answers and ends only when its context does.
type Step struct {
	Response Response
	Err      error
	// Hang makes the request wait for its context and return its error.
	Hang bool
}

// Fake is a Client that answers from a queue and records what it was asked.
// It is for tests.
type Fake struct {
	mu    sync.Mutex
	steps []Step
	seen  []Request
}

// NewFake returns a Fake that answers with steps, in order.
func NewFake(steps ...Step) *Fake { return &Fake{steps: steps} }

// Push adds steps to the end of the queue.
func (f *Fake) Push(steps ...Step) {
	f.mu.Lock()
	f.steps = append(f.steps, steps...)
	f.mu.Unlock()
}

// Complete implements Client.
func (f *Fake) Complete(ctx context.Context, req Request) (Response, error) {
	f.mu.Lock()
	// The loop appends to its message slice after the call; keep a copy.
	req.Messages = append([]Message(nil), req.Messages...)
	f.seen = append(f.seen, req)
	if len(f.steps) == 0 {
		f.mu.Unlock()
		return Response{}, errors.New("llm.Fake: no scripted response left")
	}
	s := f.steps[0]
	f.steps = f.steps[1:]
	f.mu.Unlock()
	if s.Hang {
		<-ctx.Done()
		return Response{}, ctx.Err()
	}
	if s.Err != nil {
		return Response{}, s.Err
	}
	if err := ctx.Err(); err != nil {
		return Response{}, err
	}
	return s.Response, nil
}

// Requests returns the requests seen so far.
func (f *Fake) Requests() []Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Request(nil), f.seen...)
}

// Left is how many scripted answers have not been used.
func (f *Fake) Left() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.steps)
}

// EndTurn is a step whose answer is text and the end of the turn.
func EndTurn(text string, u Usage) Step {
	return Step{Response: Response{Content: []Block{Text(text)}, StopReason: StopEndTurn, Usage: u, Model: "fake-model"}}
}

// ToolUse is a step whose answer calls tools: each call is a tool_use block
// with a generated id, name and the JSON of its input.
func ToolUse(u Usage, calls ...Call) Step {
	r := Response{StopReason: StopToolUse, Usage: u, Model: "fake-model"}
	for i, c := range calls {
		id := c.ID
		if id == "" {
			id = "toolu_" + string(rune('a'+i))
		}
		in := json.RawMessage(c.Input)
		if len(in) == 0 {
			in = json.RawMessage(`{}`)
		}
		r.Content = append(r.Content, Block{Kind: KindToolUse, ID: id, Name: c.Name, Input: in})
	}
	return Step{Response: r}
}

// Call is one scripted tool call.
type Call struct {
	ID    string
	Name  string
	Input string // JSON; empty = {}
}

// Stopped is a step that ends with the given stop reason and text.
func Stopped(reason StopReason, text string, u Usage) Step {
	r := Response{StopReason: reason, Usage: u, Model: "fake-model"}
	if text != "" {
		r.Content = []Block{Text(text)}
	}
	return Step{Response: r}
}
