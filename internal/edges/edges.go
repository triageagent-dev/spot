// Package edges turns two snapshots of the tenant's open issues into the
// facts that wake the agent early: a new or reopened issue, a severity
// change, a new comment. It costs nothing against the model.
package edges

import (
	"fmt"
	"sort"

	"github.com/triageagent-dev/tria-spot/internal/corecli"
)

// Kind is the type of an edge.
type Kind string

const (
	New      Kind = "new-issue"
	Reopened Kind = "reopened-issue"
	Severity Kind = "severity-change"
	Comment  Kind = "new-comment"
	Message  Kind = "room-message"
)

// Edge is one change worth waking for.
type Edge struct {
	Kind    Kind
	IssueID string
	Fact    string // one line the prompt shows the agent
	Human   bool   // a person is talking to the spot (comment or room message)
}

type seen struct {
	severity string
	reopens  int
	comments int
}

// Tracker remembers the last snapshot. The zero value is unprimed: its first
// Observe records a baseline and reports nothing, because the startup turn
// already looks at everything.
type Tracker struct {
	prev   map[string]seen
	primed bool
}

// Observe compares issues with the last snapshot, stores it and returns the
// edges, in a stable order.
func (t *Tracker) Observe(issues []corecli.Issue) []Edge {
	cur := make(map[string]seen, len(issues))
	var out []Edge
	for _, is := range issues {
		cur[is.ID] = seen{is.Severity, is.ReopenCount, is.Comments}
		if !t.primed {
			continue
		}
		p, known := t.prev[is.ID]
		switch {
		case !known:
			out = append(out, Edge{Kind: New, IssueID: is.ID,
				Fact: fmt.Sprintf("new issue %s (%s): %s", is.ID, is.Severity, one(is.Summary))})
			continue
		case is.ReopenCount > p.reopens:
			out = append(out, Edge{Kind: Reopened, IssueID: is.ID,
				Fact: fmt.Sprintf("issue %s was reopened (reopen count %d): %s", is.ID, is.ReopenCount, one(is.Summary))})
		}
		if is.Severity != p.severity {
			out = append(out, Edge{Kind: Severity, IssueID: is.ID,
				Fact: fmt.Sprintf("issue %s severity %s -> %s", is.ID, p.severity, is.Severity)})
		}
		if is.Comments > p.comments {
			out = append(out, Edge{Kind: Comment, IssueID: is.ID, Human: true,
				Fact: fmt.Sprintf("issue %s has %d new comment(s) since the last look (the core does not say who wrote them; read the issue)", is.ID, is.Comments-p.comments)})
		}
	}
	t.prev, t.primed = cur, true
	sort.SliceStable(out, func(i, j int) bool { return out[i].IssueID < out[j].IssueID })
	return out
}

// Rebaseline records issues as the new baseline without reporting edges. The
// supervisor calls it after a turn, so the agent's own comments and changes
// do not wake it again.
func (t *Tracker) Rebaseline(issues []corecli.Issue) {
	t.primed = false
	t.Observe(issues)
}

func one(s string) string {
	if len(s) > 160 {
		return s[:160] + "..."
	}
	return s
}
