package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeConnectors(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "connectors.json")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadConnectorsReadsAndNamesTheAllowedTools(t *testing.T) {
	p := writeConnectors(t, `[{"name":"example-kube","url":"http://tools.example.svc:8084/mcp","tools":["kubectl_get","kubectl_logs"],"hint":"The workspace's cluster, read-only."}]`)
	cs, err := LoadConnectors(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(cs) != 1 || cs[0].Name != "example-kube" {
		t.Fatalf("connectors = %+v", cs)
	}
	got := strings.Join(cs[0].AllowedRules(), ",")
	if got != "mcp__example-kube__kubectl_get,mcp__example-kube__kubectl_logs" {
		t.Fatalf("rules = %s", got)
	}
}

func TestLoadConnectorsRefusesWhatWouldWidenTheSpot(t *testing.T) {
	for name, body := range map[string]string{
		"no tools named":   `[{"name":"x","url":"http://a/mcp","tools":[]}]`,
		"core name taken":  `[{"name":"core","url":"http://a/mcp","tools":["t"]}]`,
		"ego name taken":   `[{"name":"ego","url":"http://a/mcp","tools":["t"]}]`,
		"bad name":         `[{"name":"Bad Name","url":"http://a/mcp","tools":["t"]}]`,
		"duplicate":        `[{"name":"x","url":"http://a/mcp","tools":["t"]},{"name":"x","url":"http://b/mcp","tools":["t"]}]`,
		"not http":         `[{"name":"x","url":"ftp://a/mcp","tools":["t"]}]`,
		"wildcard tool":    `[{"name":"x","url":"http://a/mcp","tools":["*"]}]`,
		"tool with a rule": `[{"name":"x","url":"http://a/mcp","tools":["t(*)"]}]`,
		"not json":         `{`,
	} {
		if _, err := LoadConnectors(writeConnectors(t, body)); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
	if cs, err := LoadConnectors(""); err != nil || cs != nil {
		t.Fatalf("no file = no connectors, got %v, %v", cs, err)
	}
}
