package config

import (
	"encoding/json"
	"strings"
	"testing"
)

// The spot's card (the core's ADR-114) says what it is for and the skills it
// declares: the given ones first, then the built-in cluster skill and one per
// connector, with a given skill replacing a built-in of the same id.
func TestCardDeclaresTheGivenTheClusterAndTheConnectorSkills(t *testing.T) {
	c := Config{Tenant: "ws-1", Name: "ws-1-spot", ChatName: "Ops", Version: "0.9.6",
		Connectors: []Connector{{Name: "Loki", URL: "http://loki", Tools: []string{"query_range", " Labels "}, Hint: "The logs of the cluster."}},
		Skills:     []Skill{{ID: "deploy", Name: "Deploy", Description: "rolls a release", Tags: []string{"argo"}}},
	}
	card := c.Card()
	if card.Name != "Ops" || card.Version != "0.9.6" || !strings.Contains(card.Description, "ws-1") || card.ProtocolVersion != "0.3.0" {
		t.Fatalf("card %+v", card)
	}
	if len(card.Capabilities.Extensions) != 1 || card.Capabilities.Extensions[0].URI != SpotExtension || !card.Capabilities.Extensions[0].Required {
		t.Fatalf("extension %+v", card.Capabilities.Extensions)
	}
	ids := make([]string, 0, len(card.Skills))
	for _, sk := range card.Skills {
		ids = append(ids, sk.ID)
	}
	if strings.Join(ids, ",") != "deploy,cluster,connector:loki" {
		t.Fatalf("skills %v", ids)
	}
	if loki := card.Skills[2]; loki.Name != "Loki" || loki.Description != "The logs of the cluster." || strings.Join(loki.Tags, ",") != "query_range,labels" {
		t.Fatalf("connector skill %+v", loki)
	}
	// the card is plain JSON for the hello
	b, err := json.Marshal(card)
	if err != nil || !strings.Contains(string(b), `"id":"deploy"`) || !strings.Contains(string(b), `"uri":"`+SpotExtension+`"`) {
		t.Fatalf("marshal: %s %v", b, err)
	}
	// a given skill replaces a built-in of the same id; a description of one's own
	c.Skills = []Skill{{ID: "cluster", Name: "Only the pods", Tags: []string{"pods"}}}
	c.Description = "Reads the pods of one namespace."
	card = c.Card()
	if card.Description != "Reads the pods of one namespace." || len(card.Skills) != 2 || card.Skills[0].Name != "Only the pods" || card.Skills[1].ID != "connector:loki" {
		t.Fatalf("card %+v", card.Skills)
	}
	// a spot with no chat name and no version
	card = Config{Tenant: "ws-1", Name: "ws-1-spot"}.Card()
	if card.Name != "ws-1-spot" || card.Version != "dev" || len(card.Skills) != 1 || card.Skills[0].ID != "cluster" {
		t.Fatalf("plain card %+v", card)
	}
}

func TestParseSkillsValidatesIdsAndCleansTags(t *testing.T) {
	got, err := ParseSkills(`[{"id":" Logs ","tags":["Loki","","loki"," Pods "]},{"id":"metrics","name":"Metrics","description":" prom "}]`)
	if err != nil || len(got) != 2 {
		t.Fatalf("%+v %v", got, err)
	}
	if got[0].ID != "logs" || got[0].Name != "logs" || strings.Join(got[0].Tags, ",") != "loki,pods" || got[1].Description != "prom" {
		t.Fatalf("%+v", got)
	}
	for _, bad := range []string{`[{"name":"no id"}]`, `[{"id":"a"},{"id":"A"}]`, `{"id":"not a list"}`} {
		if _, err := ParseSkills(bad); err == nil {
			t.Fatalf("%s was taken", bad)
		}
	}
	many := make([]Skill, maxSkills+1)
	for i := range many {
		many[i].ID = "s" + strings.Repeat("x", i)
	}
	b, _ := json.Marshal(many)
	if _, err := ParseSkills(string(b)); err == nil {
		t.Fatal("more skills than the bound were taken")
	}
	if got, err := ParseSkills("  "); err != nil || got != nil {
		t.Fatalf("empty: %+v %v", got, err)
	}
	if _, err := LoadSkills("/nonexistent/skills.json"); err == nil {
		t.Fatal("a missing file was taken")
	}
}

// --skills-json and --skills-file are read by Parse and cannot both be given.
func TestParseTakesTheSkillsFlags(t *testing.T) {
	base := []string{"--tenant", "ws-1", "--core-mcp-url", "http://core/mcp", "--core-token-file", "/dev/null", "--anthropic-key-file", "/dev/null", "--instruction-cell", "cell"}
	c, err := Parse(append(base, "--skills-json", `[{"id":"deploy"}]`, "--description", "Rolls releases."), nil)
	if err != nil || len(c.Skills) != 1 || c.Skills[0].ID != "deploy" || c.Description != "Rolls releases." {
		t.Fatalf("%+v %v", c.Skills, err)
	}
	if _, err := Parse(append(base, "--skills-json", `[{"id":"deploy"}]`, "--skills-file", "/dev/null"), nil); err == nil {
		t.Fatal("both flags were taken")
	}
	if _, err := Parse(append(base, "--skills-json", `[{"name":"no id"}]`), nil); err == nil {
		t.Fatal("a skill without an id was taken")
	}
}
