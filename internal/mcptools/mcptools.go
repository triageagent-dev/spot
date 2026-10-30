// Package mcptools is the spot's client for the MCP servers it uses as tool
// sources: the core, ego and the connectors. It speaks streamable HTTP with
// github.com/mark3labs/mcp-go and sends the requests it needs as raw JSON-RPC,
// so a tool's schema reaches the model exactly as the server wrote it, and a
// schema the typed client cannot parse (a JSON Schema "type" that is a list)
// still lists and still calls.
package mcptools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/client/transport"
	"github.com/mark3labs/mcp-go/mcp"

	"github.com/triageagent-dev/spot/internal/injected"
)

// Tool is one tool a server offers.
type Tool struct {
	Name        string
	Description string
	// InputSchema is the tool's JSON schema as the server sent it.
	InputSchema json.RawMessage
}

// Result is what a tool call returned.
type Result struct {
	// Text is the text blocks of the result joined by newlines; a block that is
	// not text becomes a one-line placeholder.
	Text    string
	IsError bool
}

// Conn is one initialized MCP session.
type Conn struct {
	cl     *client.Client
	nextID atomic.Int64
}

// Connect opens a session with the server at url. If tokenFile is not empty its
// content is sent as a bearer token, read again on every request, so a rotated
// Secret is picked up without a restart. A connector carries no token: it holds
// its own credentials. httpTimeout bounds each HTTP request.
func Connect(ctx context.Context, url, tokenFile string, httpTimeout time.Duration) (*Conn, error) {
	opts := []transport.StreamableHTTPCOption{transport.WithHTTPTimeout(httpTimeout)}
	if tokenFile != "" {
		opts = append(opts, transport.WithHTTPHeaderFunc(func(context.Context) map[string]string {
			return map[string]string{"Authorization": "Bearer " + readToken(tokenFile)}
		}))
	}
	cl, err := client.NewStreamableHttpClient(url, opts...)
	if err != nil {
		return nil, err
	}
	if err := cl.Start(ctx); err != nil {
		return nil, err
	}
	init := mcp.InitializeRequest{}
	init.Params.ProtocolVersion = mcp.LATEST_PROTOCOL_VERSION
	init.Params.ClientInfo = mcp.Implementation{Name: "tria-spot", Version: "1"}
	if _, err := cl.Initialize(ctx, init); err != nil {
		_ = cl.Close()
		return nil, fmt.Errorf("initialize: %w", err)
	}
	c := &Conn{cl: cl}
	c.nextID.Store(1 << 20) // apart from the ids the typed client uses
	return c, nil
}

func readToken(path string) string {
	if tok, ok := injected.Read(path); ok {
		return tok
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// Close ends the session.
func (c *Conn) Close() { _ = c.cl.Close() }

func (c *Conn) request(ctx context.Context, method string, params any) (json.RawMessage, error) {
	resp, err := c.cl.GetTransport().SendRequest(ctx, transport.JSONRPCRequest{
		JSONRPC: "2.0", ID: mcp.NewRequestId(c.nextID.Add(1)), Method: method, Params: params,
	})
	if err != nil {
		return nil, fmt.Errorf("%s: %w", method, err)
	}
	if resp.Error != nil {
		return nil, fmt.Errorf("%s: %s", method, resp.Error.Message)
	}
	return resp.Result, nil
}

// Tools lists every tool the server offers, sorted by name.
func (c *Conn) Tools(ctx context.Context) ([]Tool, error) {
	var out []Tool
	cursor := ""
	for {
		var params any
		if cursor != "" {
			params = map[string]any{"cursor": cursor}
		}
		raw, err := c.request(ctx, "tools/list", params)
		if err != nil {
			return nil, err
		}
		var page struct {
			Tools []struct {
				Name        string          `json:"name"`
				Description string          `json:"description"`
				InputSchema json.RawMessage `json:"inputSchema"`
			} `json:"tools"`
			NextCursor string `json:"nextCursor"`
		}
		if err := json.Unmarshal(raw, &page); err != nil {
			return nil, fmt.Errorf("tools/list: %w", err)
		}
		for _, t := range page.Tools {
			out = append(out, Tool{Name: t.Name, Description: t.Description, InputSchema: t.InputSchema})
		}
		if page.NextCursor == "" || page.NextCursor == cursor {
			break
		}
		cursor = page.NextCursor
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Call runs a tool with args (a JSON object; empty = none). A tool that
// reports an error returns it in Result.IsError; err is for the transport and
// the protocol.
func (c *Conn) Call(ctx context.Context, name string, args json.RawMessage) (Result, error) {
	p := map[string]any{"name": name}
	if len(strings.TrimSpace(string(args))) > 0 && string(args) != "null" {
		p["arguments"] = args
	}
	raw, err := c.request(ctx, "tools/call", p)
	if err != nil {
		return Result{}, err
	}
	var r struct {
		Content []struct {
			Type     string `json:"type"`
			Text     string `json:"text"`
			MIMEType string `json:"mimeType"`
			Data     string `json:"data"`
			Resource struct {
				URI  string `json:"uri"`
				Text string `json:"text"`
			} `json:"resource"`
		} `json:"content"`
		Structured json.RawMessage `json:"structuredContent"`
		IsError    bool            `json:"isError"`
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		return Result{}, fmt.Errorf("tools/call: %w", err)
	}
	var parts []string
	for _, b := range r.Content {
		switch b.Type {
		case "text":
			parts = append(parts, b.Text)
		case "image", "audio":
			parts = append(parts, fmt.Sprintf("[%s content omitted: %s, %d bytes encoded]", b.Type, orUnknown(b.MIMEType), len(b.Data)))
		case "resource":
			if b.Resource.Text != "" {
				parts = append(parts, b.Resource.Text)
			} else {
				parts = append(parts, fmt.Sprintf("[resource omitted: %s]", orUnknown(b.Resource.URI)))
			}
		default:
			parts = append(parts, fmt.Sprintf("[%s content omitted]", orUnknown(b.Type)))
		}
	}
	if len(parts) == 0 && len(r.Structured) > 0 && string(r.Structured) != "null" {
		parts = append(parts, string(r.Structured))
	}
	return Result{Text: strings.Join(parts, "\n"), IsError: r.IsError}, nil
}

func orUnknown(s string) string {
	if s == "" {
		return "unknown"
	}
	return s
}
