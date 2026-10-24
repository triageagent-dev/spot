// Package mcptools lists the tools an MCP server offers. The supervisor uses
// it to turn a connector's allowlist into a denylist: a tool the connector
// offers and the spot does not name must be refused by name in every
// permission mode, not left to the CLI's classifier.
package mcptools

import (
	"context"
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
	var names []string
	req := mcp.ListToolsRequest{}
	for {
		res, err := cl.ListTools(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("tools/list: %w", err)
		}
		for _, t := range res.Tools {
			names = append(names, t.Name)
		}
		if res.NextCursor == "" {
			break
		}
		req.Params.Cursor = res.NextCursor
	}
	sort.Strings(names)
	return names, nil
}
