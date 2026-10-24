package runner

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/triageagent-dev/tria-spot/internal/render"
)

// connectorServer serves tools a, b and c over MCP and counts the requests.
func connectorServer(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	s := server.NewMCPServer("example", "1")
	for _, n := range []string{"a", "b", "c"} {
		s.AddTool(mcp.NewTool(n), func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return mcp.NewToolResultText("ok"), nil
		})
	}
	var n atomic.Int64
	h := server.NewStreamableHTTPServer(s)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	return ts, &n
}

func gateOpts(t *testing.T, mode string, log *bytes.Buffer, conns ...render.Server) Options {
	t.Helper()
	d := t.TempDir()
	token := filepath.Join(d, "core-token")
	if err := os.WriteFile(token, []byte("core-token\n"), 0o400); err != nil {
		t.Fatal(err)
	}
	o := opts()
	o.PermissionMode = mode
	o.Paths.MCPConfig = filepath.Join(d, "mcp.json")
	o.CoreMCPURL = "https://core.example.invalid/mcp"
	o.CoreTokenFile = token
	o.Connectors = conns
	o.Logger = slog.New(slog.NewTextHandler(log, nil))
	return o
}

func TestGatedDisallowsUnnamedConnectorTools(t *testing.T) {
	ts, _ := connectorServer(t)
	var log bytes.Buffer
	o := gateOpts(t, "auto", &log, render.Server{Name: "example-kube", URL: ts.URL, Tools: []string{"a"}})

	g := o.gated(context.Background())
	args := strings.Join(BuildArgs(g, ""), "\x00")
	for _, d := range DefaultDisallowed() {
		if !strings.Contains(args, d) {
			t.Errorf("the default list lost %q: %s", d, args)
		}
	}
	for _, want := range []string{"mcp__example-kube__b", "mcp__example-kube__c"} {
		if !strings.Contains(args, want) {
			t.Errorf("missing %q in %s", want, args)
		}
	}
	if strings.Contains(args, "mcp__example-kube__a") {
		t.Errorf("an allowed tool is disallowed: %s", args)
	}
	if len(g.Connectors) != 1 {
		t.Errorf("connector dropped: %+v", g.Connectors)
	}
	// The same tool set gives the same arguments, so a kept process is not replaced.
	if a, b := strings.Join(BuildArgs(o.gated(context.Background()), ""), "\x00"), args; a != b {
		t.Errorf("not deterministic:\n%s\n%s", a, b)
	}
	if len(o.DisallowedTools) != 0 {
		t.Error("the caller's options were changed")
	}
}

func TestGatedLeavesOutAnUnreachableConnector(t *testing.T) {
	ts, _ := connectorServer(t)
	var log bytes.Buffer
	o := gateOpts(t, "auto", &log,
		render.Server{Name: "example-kube", URL: ts.URL, Tools: []string{"a"}},
		render.Server{Name: "example-down", URL: "http://127.0.0.1:1/mcp", Tools: []string{"a"}})

	g := o.gated(context.Background())
	if len(g.Connectors) != 1 || g.Connectors[0].Name != "example-kube" {
		t.Fatalf("connectors = %+v", g.Connectors)
	}
	if err := writeMCP(g); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(o.Paths.MCPConfig)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "example-kube") || strings.Contains(string(b), "example-down") {
		t.Errorf("mcp file: %s", b)
	}
	if l := log.String(); !strings.Contains(l, "level=ERROR") || !strings.Contains(l, "connector=example-down") || !strings.Contains(l, "err=") {
		t.Errorf("log: %s", l)
	}
}

func TestGatedDontAskListsNothing(t *testing.T) {
	ts, n := connectorServer(t)
	var log bytes.Buffer
	o := gateOpts(t, "dontAsk", &log,
		render.Server{Name: "example-kube", URL: ts.URL, Tools: []string{"a"}},
		render.Server{Name: "example-down", URL: "http://127.0.0.1:1/mcp", Tools: []string{"a"}})

	g := o.gated(context.Background())
	if n.Load() != 0 {
		t.Errorf("%d requests in dontAsk", n.Load())
	}
	if len(g.DisallowedTools) != 0 || len(g.Connectors) != 2 {
		t.Errorf("dontAsk changed the options: %+v", g)
	}
	if log.Len() != 0 {
		t.Errorf("log: %s", log.String())
	}
}
