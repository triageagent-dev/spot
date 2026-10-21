// Package stream reads the Claude Code CLI's --output-format stream-json
// output (one JSON object per line) and reduces it to what the supervisor
// needs: the session id, the permission mode the CLI really started in, the
// final result and token usage.
//
// Field names follow the SDK message types documented at
// https://code.claude.com/docs/en/agent-sdk/typescript (SDKSystemMessage,
// SDKAssistantMessage, SDKResultMessage).
package stream

import (
	"bufio"
	"encoding/json"
	"io"
	"strconv"
)

// Usage is a token count in the Anthropic usage shape.
type Usage struct {
	InputTokens         int64 `json:"input_tokens"`
	OutputTokens        int64 `json:"output_tokens"`
	CacheCreationTokens int64 `json:"cache_creation_input_tokens"`
	CacheReadTokens     int64 `json:"cache_read_input_tokens"`
}

// Billable is input + output + cache creation. Cache reads are tracked but
// not counted against the daily ceiling: on a resumed conversation every
// turn re-reads the whole context, and the reads bill at a fraction of the
// input rate.
func (u Usage) Billable() int64 { return u.InputTokens + u.OutputTokens + u.CacheCreationTokens }

func (u *Usage) add(o Usage) {
	u.InputTokens += o.InputTokens
	u.OutputTokens += o.OutputTokens
	u.CacheCreationTokens += o.CacheCreationTokens
	u.CacheReadTokens += o.CacheReadTokens
}

// MCPServer is one entry of the init message's mcp_servers.
type MCPServer struct {
	Name   string `json:"name"`
	Status string `json:"status"`
}

// Summary is what one CLI invocation reported.
type Summary struct {
	SessionID      string
	Model          string
	PermissionMode string
	MCPServers     []MCPServer
	GotInit        bool

	GotResult         bool
	Subtype           string
	IsError           bool
	ResultText        string
	Errors            []string
	TerminalReason    string
	NumTurns          int
	PermissionDenials int
	APIErrorStatus    int
	CostUSD           float64 // total_cost_usd: cumulative over a resumed conversation
	ResultUsage       Usage

	// AssistantUsage sums the usage of each distinct assistant message. It is
	// the fallback when the process dies before it writes a result.
	AssistantUsage Usage
}

// Tokens returns the invocation's usage: the result's if there was one, else
// the sum over assistant messages.
func (s Summary) Tokens() Usage {
	if s.GotResult {
		return s.ResultUsage
	}
	return s.AssistantUsage
}

type line struct {
	Type              string            `json:"type"`
	Subtype           string            `json:"subtype"`
	SessionID         string            `json:"session_id"`
	Model             string            `json:"model"`
	PermissionMode    string            `json:"permissionMode"`
	MCPServers        []MCPServer       `json:"mcp_servers"`
	IsError           bool              `json:"is_error"`
	Result            string            `json:"result"`
	Errors            []string          `json:"errors"`
	TerminalReason    string            `json:"terminal_reason"`
	NumTurns          int               `json:"num_turns"`
	APIErrorStatus    *int              `json:"api_error_status"`
	TotalCostUSD      float64           `json:"total_cost_usd"`
	Usage             *Usage            `json:"usage"`
	PermissionDenials []json.RawMessage `json:"permission_denials"`
	Message           *struct {
		ID    string `json:"id"`
		Usage *Usage `json:"usage"`
	} `json:"message"`
}

// MaxLine bounds one stream line; tool results can be large.
const MaxLine = 32 << 20

// Parse reads r to EOF. Lines that are not JSON objects are skipped: the CLI
// may print a banner or a warning, and a parse error must not lose the
// result. onLine, if not nil, sees every raw line (for debug logging).
func Parse(r io.Reader, onLine func([]byte)) (Summary, error) {
	var s Summary
	perMsg := map[string]Usage{}
	br := bufio.NewReaderSize(r, 1<<20)
	for {
		raw, err := readLine(br)
		if len(raw) > 0 {
			if onLine != nil {
				onLine(raw)
			}
			var l line
			if json.Unmarshal(raw, &l) == nil && l.Type != "" {
				s.apply(l, perMsg)
			}
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return s, err
		}
	}
	for _, u := range perMsg {
		s.AssistantUsage.add(u)
	}
	return s, nil
}

func (s *Summary) apply(l line, perMsg map[string]Usage) {
	if l.SessionID != "" {
		s.SessionID = l.SessionID
	}
	switch l.Type {
	case "system":
		if l.Subtype == "init" {
			s.GotInit = true
			s.Model = l.Model
			s.PermissionMode = l.PermissionMode
			s.MCPServers = l.MCPServers
		}
	case "assistant":
		if l.Message != nil && l.Message.Usage != nil {
			id := l.Message.ID
			if id == "" {
				id = "anon-" + strconv.Itoa(len(perMsg))
			}
			prev := perMsg[id]
			cur := *l.Message.Usage
			// Streaming repeats a message with growing usage: keep the largest.
			if cur.OutputTokens >= prev.OutputTokens {
				perMsg[id] = cur
			}
		}
	case "result":
		s.GotResult = true
		s.Subtype = l.Subtype
		s.IsError = l.IsError || (l.Subtype != "" && l.Subtype != "success")
		s.ResultText = l.Result
		s.Errors = l.Errors
		s.TerminalReason = l.TerminalReason
		s.NumTurns = l.NumTurns
		s.PermissionDenials = len(l.PermissionDenials)
		if l.APIErrorStatus != nil {
			s.APIErrorStatus = *l.APIErrorStatus
		}
		s.CostUSD = l.TotalCostUSD
		if l.Usage != nil {
			s.ResultUsage = *l.Usage
		}
	}
}

// readLine returns one line without its newline, however long (up to
// MaxLine), and the read error, if any, with the final partial line.
func readLine(br *bufio.Reader) ([]byte, error) {
	var out []byte
	for {
		chunk, isPrefix, err := br.ReadLine()
		if len(out)+len(chunk) <= MaxLine {
			out = append(out, chunk...)
		}
		if err != nil {
			return out, err
		}
		if !isPrefix {
			return out, nil
		}
	}
}
