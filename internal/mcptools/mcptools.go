// Package mcptools lists the tools an MCP server offers. The supervisor uses
// it to turn a connector's allowlist into a denylist: a tool the connector
// offers and the spot does not name must be refused by name in every
// permission mode, not left to the CLI's classifier.
package mcptools

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/client/transport"
	"github.com/mark3labs/mcp-go/mcp"
)

// List returns the sorted names of the tools the MCP server at url offers. It
// sends no credential: a connector holds its own. The client is closed before
// List returns.
func List(ctx context.Context, url string) ([]string, error) {
	cl, err := client.NewStreamableHttpClient(url, transport.WithHTTPTimeout(15*time.Second))
	if err != nil {
		return nil, err
	}
	defer func() { _ = cl.Close() }()
	if err := cl.Start(ctx); err != nil {
		return nil, err
	}
	init := mcp.InitializeRequest{}
	init.Params.ProtocolVersion = mcp.LATEST_PROTOCOL_VERSION
	init.Params.ClientInfo = mcp.Implementation{Name: "tria-spot-supervisor", Version: "1"}
	if _, err := cl.Initialize(ctx, init); err != nil {
		return nil, fmt.Errorf("initialize: %w", err)
	}
	// The names only: a tool's schema is not needed here, and a server whose
	// schemas the typed client cannot parse (a JSON Schema "type" that is a list)
	// must still be listable, or its connector would be dropped.
	var names []string
	cursor := ""
	for i := 1; ; i++ {
		var params any
		if cursor != "" {
			params = map[string]any{"cursor": cursor}
		}
		resp, err := cl.GetTransport().SendRequest(ctx, transport.JSONRPCRequest{
			JSONRPC: "2.0", ID: mcp.NewRequestId(int64(1000 + i)), Method: "tools/list", Params: params,
		})
		if err != nil {
			return nil, fmt.Errorf("tools/list: %w", err)
		}
		if resp.Error != nil {
			return nil, fmt.Errorf("tools/list: %s", resp.Error.Message)
		}
		var page struct {
			Tools []struct {
				Name string `json:"name"`
			} `json:"tools"`
			NextCursor string `json:"nextCursor"`
		}
		if err := json.Unmarshal(resp.Result, &page); err != nil {
			return nil, fmt.Errorf("tools/list: %w", err)
		}
		for _, t := range page.Tools {
			names = append(names, t.Name)
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	sort.Strings(names)
	return names, nil
}
