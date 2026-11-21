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
// list of its tool names the spot may call; any other tool the connector
// offers is not given to the model. The model sees them as <name>__<tool>.
type Connector struct {
	Name  string   `json:"name"`
	URL   string   `json:"url"`
	Tools []string `json:"tools"`
	// Hint is one or two plain sentences for the spot's prompt: what the
	// connector reaches and when to use it.
	Hint string `json:"hint"`
	// Writes names the tools in Tools that change state. A turn whose standing
	// notes were read may call the others at once; a write still waits for the
	// model's search of the subject. Empty = no tool is known to be a read.
	Writes []string `json:"writes,omitempty"`
	// Auth is empty, or AuthInjected: a substrate actor holds no credential, so
	// it sends the placeholder bearer and the egress in front of the connector
	// replaces it (with an actor JWT or a Secret's value). Without it no header
	// is sent and the egress has nothing to replace.
	Auth string `json:"auth,omitempty"`
}

// ConnectorAuthInjected is the one Auth a connector may name.
const ConnectorAuthInjected = "injected"

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
	cs, err := parseConnectors(b, "--connectors-file")
	return cs, err
}

// ParseConnectors reads and validates connectors given as the JSON itself
// (--connectors-json), for a runtime that has no file to read one from.
func ParseConnectors(js string) ([]Connector, error) {
	if strings.TrimSpace(js) == "" {
		return nil, nil
	}
	return parseConnectors([]byte(js), "--connectors-json")
}

func parseConnectors(b []byte, flagName string) ([]Connector, error) {
	var cs []Connector
	if err := json.Unmarshal(b, &cs); err != nil {
		return nil, fmt.Errorf("%s: %w", flagName, err)
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
		if c.Auth != "" && c.Auth != ConnectorAuthInjected {
			errs = append(errs, fmt.Errorf("%s: auth %q is not %q", where, c.Auth, ConnectorAuthInjected))
		}
		for _, w := range c.Writes {
			found := false
			for _, t := range c.Tools {
				found = found || t == w
			}
			if !found {
				errs = append(errs, fmt.Errorf("%s: write %q is not one of its tools", where, w))
			}
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
