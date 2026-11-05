package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/a2aproject/a2a-go/a2a"
)

// A spot is an A2A agent of its workspace (the core's ADR-114): it tells the
// core what it is for and what it can do with an agent card, sent on every
// hello. Tria reads the cards of a workspace's spots and names the spot, or the
// skill, when she asks; a workspace of many spots is told apart by its cards.

// SpotExtension is the A2A extension a spot's card declares.
const SpotExtension = "https://triageagent.dev/a2a/spot/v1"

// maxSkills bounds the skills a spot declares, the core keeps that many.
const maxSkills = 32

// Skill is one declared skill: what the spot can do, as an A2A AgentSkill.
type Skill struct {
	ID          string   `json:"id"`
	Name        string   `json:"name,omitempty"`
	Description string   `json:"description,omitempty"`
	Tags        []string `json:"tags,omitempty"`
}

// ParseSkills reads and validates a skills list: a JSON array of Skill. An id
// is required and unique (case aside); a name defaults to the id.
func ParseSkills(js string) ([]Skill, error) {
	if strings.TrimSpace(js) == "" {
		return nil, nil
	}
	var in []Skill
	if err := json.Unmarshal([]byte(js), &in); err != nil {
		return nil, fmt.Errorf("skills: %w", err)
	}
	if len(in) > maxSkills {
		return nil, fmt.Errorf("skills: at most %d, got %d", maxSkills, len(in))
	}
	seen := map[string]bool{}
	out := make([]Skill, 0, len(in))
	for i, sk := range in {
		sk.ID = strings.ToLower(strings.TrimSpace(sk.ID))
		if sk.ID == "" {
			return nil, fmt.Errorf("skills[%d]: an id is required", i)
		}
		if seen[sk.ID] {
			return nil, fmt.Errorf("skills[%d]: the id %q is given twice", i, sk.ID)
		}
		seen[sk.ID] = true
		if sk.Name = strings.TrimSpace(sk.Name); sk.Name == "" {
			sk.Name = sk.ID
		}
		sk.Description = strings.TrimSpace(sk.Description)
		sk.Tags = cleanTags(sk.Tags)
		out = append(out, sk)
	}
	return out, nil
}

// LoadSkills reads a skills file; an empty path is no skills.
func LoadSkills(path string) ([]Skill, error) {
	if path == "" {
		return nil, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("skills file: %w", err)
	}
	return ParseSkills(string(b))
}

// cleanTags lower-cases and trims tags and drops empty ones and repeats.
func cleanTags(tags []string) []string {
	var out []string
	seen := map[string]bool{}
	for _, t := range tags {
		t = strings.ToLower(strings.TrimSpace(t))
		if t == "" || seen[t] {
			continue
		}
		seen[t] = true
		out = append(out, t)
	}
	return out
}

// Card is the spot's agent card: its name, what it is for, its version and the
// skills it declares: the built-in cluster skill, one per connector (id
// connector:<name>, the tools as tags, the hint as the description) and the
// skills it was given, in that order, the given ones ahead of a built-in with
// the same id. The card is sent with every hello.
func (c Config) Card() *a2a.AgentCard {
	name := c.ChatName
	if name == "" {
		name = c.Name
	}
	desc := strings.TrimSpace(c.Description)
	if desc == "" {
		desc = "The hands of workspace " + c.Tenant + " in its cluster: looks at live pods, logs, services and events, and reaches its connectors."
	}
	version := c.Version
	if version == "" {
		version = "dev"
	}
	skills := []Skill{{
		ID: "cluster", Name: "Look inside the cluster",
		Description: "Live pods, logs, services, events and the state of one workload, through the core's cluster tools.",
		Tags:        []string{"kubernetes", "pods", "logs", "services", "events"},
	}}
	for _, cn := range c.Connectors {
		skills = append(skills, Skill{
			ID: "connector:" + strings.ToLower(cn.Name), Name: cn.Name,
			Description: strings.TrimSpace(cn.Hint),
			Tags:        cleanTags(cn.Tools),
		})
	}
	given := map[string]bool{}
	for _, sk := range c.Skills {
		given[sk.ID] = true
	}
	out := make([]a2a.AgentSkill, 0, len(skills)+len(c.Skills))
	add := func(sk Skill) {
		if len(out) < maxSkills {
			out = append(out, a2a.AgentSkill{ID: sk.ID, Name: sk.Name, Description: sk.Description, Tags: sk.Tags})
		}
	}
	for _, sk := range c.Skills {
		add(sk)
	}
	for _, sk := range skills {
		if !given[sk.ID] {
			add(sk)
		}
	}
	return &a2a.AgentCard{
		Name:            name,
		Description:     desc,
		Version:         version,
		ProtocolVersion: "0.3.0",
		Capabilities: a2a.AgentCapabilities{
			Extensions: []a2a.AgentExtension{{URI: SpotExtension, Required: true, Description: "A spot of a Triage workspace: asked in its bus room, answers on its own schedule."}},
		},
		DefaultInputModes:  []string{"text/plain"},
		DefaultOutputModes: []string{"text/plain"},
		Skills:             out,
	}
}

// errSkillsTwice is the error for both skill flags at once.
var errSkillsTwice = errors.New("--skills-file and --skills-json cannot both be given")
