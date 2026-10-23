package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
)

// Connector is one more MCP server the spot may use beside the core: a
// streamable-HTTP endpoint of a service that holds its own credentials and
// bounds (a Kubernetes tool server for another cluster, a code host, a
// metrics API). The spot carries no credential for it; the connector's own
// identity, RBAC and the NetworkPolicy are the boundary. Tools is the exact
// list of its tool names the spot may call; anything else is refused by name.
type Connector struct {
	Name  string   `json:"name"`
	URL   string   `json:"url"`
	Tools []string `json:"tools"`
	// Hint is one or two plain sentences for the spot's prompt: what the
	// connector reaches and when to use it.
	Hint string `json:"hint"`
}

var (
	connectorName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,29}$`)
	toolName      = regexp.MustCompile(`^[A-Za-z0-9_]+$`)
)

// reservedServers are MCP server names the spot uses itself.
var reservedServers = map[string]bool{"core": true, "ego": true}

// LoadConnectors reads and validates a connectors file: a JSON array of
// Connector. An empty path means none.
func LoadConnectors(path string) ([]Connector, error) {
	if path == "" {
		return nil, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("--connectors-file: %w", err)
	}
	var cs []Connector
	if err := json.Unmarshal(b, &cs); err != nil {
		return nil, fmt.Errorf("--connectors-file: %w", err)
	}
	seen := map[string]bool{}
	var errs []error
	for i, c := range cs {
		where := fmt.Sprintf("connector %d (%q)", i, c.Name)
		switch {
		case !connectorName.MatchString(c.Name):
			errs = append(errs, fmt.Errorf("%s: name must be lowercase letters, digits and dashes, starting with a letter", where))
		case reservedServers[c.Name]:
			errs = append(errs, fmt.Errorf("%s: the name is used by the spot itself", where))
		case seen[c.Name]:
			errs = append(errs, fmt.Errorf("%s: duplicate name", where))
		}
		seen[c.Name] = true
		if !strings.HasPrefix(c.URL, "http://") && !strings.HasPrefix(c.URL, "https://") {
			errs = append(errs, fmt.Errorf("%s: url must be http:// or https://", where))
		}
		if len(c.Tools) == 0 {
			errs = append(errs, fmt.Errorf("%s: tools must name the tools the spot may call; none means the connector is unused", where))
		}
		for _, t := range c.Tools {
			if !toolName.MatchString(t) {
				errs = append(errs, fmt.Errorf("%s: tool %q is not a plain tool name", where, t))
			}
		}
	}
	return cs, errors.Join(errs...)
}

// AllowedRules is the --allowedTools rules for the connector's tools.
func (c Connector) AllowedRules() []string {
	out := make([]string, 0, len(c.Tools))
	for _, t := range c.Tools {
		out = append(out, "mcp__"+c.Name+"__"+t)
	}
	return out
}
