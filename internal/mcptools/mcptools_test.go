package mcptools

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

func TestList(t *testing.T) {
	s := server.NewMCPServer("example", "1")
	for _, n := range []string{"c", "a", "b"} {
		s.AddTool(mcp.NewTool(n), func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return mcp.NewToolResultText("ok"), nil
		})
	}
	ts := server.NewTestStreamableHTTPServer(s)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	got, err := List(ctx, ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"a", "b", "c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("tools = %v, want %v", got, want)
	}
}

func TestListUnreachable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := List(ctx, "http://127.0.0.1:1/mcp"); err == nil {
		t.Fatal("an unreachable server must be an error")
	}
}

// A server whose tool schemas the typed client cannot parse (a "type" that is
// a list) is still listable by name.
func TestListToleratesSchemasTheClientCannotParse(t *testing.T) {
	s := server.NewMCPServer("example", "1")
	for _, n := range []string{"b", "a"} {
		s.AddTool(mcp.NewToolWithRawSchema(n, "d", json.RawMessage(`{"type":"object","properties":{"x":{"type":["string","null"]}}}`)),
			func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				return mcp.NewToolResultText("ok"), nil
			})
	}
	s.AddTool(mcp.NewToolWithRawSchema("c", "d", json.RawMessage(`{"type":["object"]}`)),
		func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return mcp.NewToolResultText("ok"), nil
		})
	ts := server.NewTestStreamableHTTPServer(s)
	defer ts.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	got, err := List(ctx, ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"a", "b", "c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("tools = %v, want %v", got, want)
	}
}
