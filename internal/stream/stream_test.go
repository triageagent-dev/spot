package stream

import (
	"strings"
	"testing"
)

const sample = `not json: a banner
{"type":"system","subtype":"init","session_id":"s-1","model":"m","permissionMode":"auto","mcp_servers":[{"name":"core","status":"connected"}]}
{"type":"assistant","session_id":"s-1","message":{"id":"msg_1","usage":{"input_tokens":10,"output_tokens":1,"cache_creation_input_tokens":100,"cache_read_input_tokens":1000}}}
{"type":"assistant","session_id":"s-1","message":{"id":"msg_1","usage":{"input_tokens":10,"output_tokens":20,"cache_creation_input_tokens":100,"cache_read_input_tokens":1000}}}
{"type":"assistant","session_id":"s-1","message":{"id":"msg_2","usage":{"input_tokens":5,"output_tokens":7}}}
{"type":"result","subtype":"success","is_error":false,"session_id":"s-1","num_turns":3,"result":"done","total_cost_usd":0.42,"usage":{"input_tokens":15,"output_tokens":27,"cache_creation_input_tokens":100,"cache_read_input_tokens":1000},"permission_denials":[{"tool_name":"Bash"}]}
`

func TestParseResult(t *testing.T) {
	s, err := Parse(strings.NewReader(sample), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !s.GotInit || s.SessionID != "s-1" || s.PermissionMode != "auto" || s.MCPServers[0].Status != "connected" {
		t.Fatalf("init: %+v", s)
	}
	if !s.GotResult || s.IsError || s.NumTurns != 3 || s.ResultText != "done" || s.PermissionDenials != 1 || s.CostUSD != 0.42 {
		t.Fatalf("result: %+v", s)
	}
	u := s.Tokens()
	if u.Billable() != 15+27+100 || u.CacheReadTokens != 1000 {
		t.Fatalf("usage: %+v", u)
	}
	// Assistant-message sum counts msg_1 once, at its largest.
	if a := s.AssistantUsage; a.OutputTokens != 27 || a.InputTokens != 15 {
		t.Fatalf("assistant usage: %+v", a)
	}
}

func TestParseWithoutResultFallsBackToAssistantUsage(t *testing.T) {
	cut := sample[:strings.Index(sample, `{"type":"result"`)]
	s, err := Parse(strings.NewReader(cut), nil)
	if err != nil {
		t.Fatal(err)
	}
	if s.GotResult {
		t.Fatal("no result expected")
	}
	if got := s.Tokens().Billable(); got != 15+27+100 {
		t.Fatalf("fallback billable = %d", got)
	}
}

func TestParseErrorSubtype(t *testing.T) {
	in := `{"type":"result","subtype":"error_max_turns","is_error":false,"session_id":"s","errors":["limit"],"terminal_reason":"max_turns","usage":{"input_tokens":1,"output_tokens":1}}`
	s, _ := Parse(strings.NewReader(in), nil)
	if !s.IsError || s.Subtype != "error_max_turns" || s.TerminalReason != "max_turns" || s.Errors[0] != "limit" {
		t.Fatalf("%+v", s)
	}
}

func TestParseVeryLongLine(t *testing.T) {
	big := strings.Repeat("x", 3<<20)
	in := `{"type":"user","message":{"content":"` + big + `"}}` + "\n" + `{"type":"result","subtype":"success","session_id":"s","usage":{"input_tokens":2,"output_tokens":3}}` + "\n"
	s, err := Parse(strings.NewReader(in), nil)
	if err != nil || !s.GotResult || s.Tokens().Billable() != 5 {
		t.Fatalf("err=%v %+v", err, s)
	}
}

func TestLastContextIsTheLastRequestsInputSide(t *testing.T) {
	s, err := Parse(strings.NewReader(sample), nil)
	if err != nil {
		t.Fatal(err)
	}
	// msg_2 is the last request: 5 input tokens, no cache.
	if s.LastContext != 5 {
		t.Fatalf("LastContext = %d, want 5", s.LastContext)
	}
	two := `{"type":"assistant","session_id":"s","message":{"id":"a","usage":{"input_tokens":10,"output_tokens":1,"cache_creation_input_tokens":100,"cache_read_input_tokens":1000}}}` + "\n"
	s, _ = Parse(strings.NewReader(two), nil)
	if s.LastContext != 1110 {
		t.Fatalf("LastContext = %d, want input + cache read + cache creation = 1110", s.LastContext)
	}
}
