// Package edges turns two snapshots of the tenant's open issues into the
// facts that wake the agent early: a new or reopened issue, a severity
// change, a new comment. It costs nothing against the model.
package edges

import (
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

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
	// Mentioned is set on a room message that names the spot.
	Mentioned bool
}

type seen struct {
	severity string
	reopens  int
	comments int
}

// Snapshot is the issue state a Tracker compares against.
type Snapshot map[string]seen

// Tracker remembers the last snapshot. The zero value is unprimed: its first
// Observe records a baseline and reports nothing, because the startup turn
// already looks at everything.
type Tracker struct {
	prev   Snapshot
	primed bool
}

// Observe compares issues with the last snapshot, stores it and returns the
// edges, in a stable order.
func (t *Tracker) Observe(issues []corecli.Issue) []Edge {
	cur := snapshot(issues)
	var out []Edge
	if t.primed {
		out = diff(t.prev, cur, issues, true)
	}
	t.prev, t.primed = cur, true
	return out
}

// Snapshot returns the last snapshot, for SettleFrom. Nil before the first
// Observe.
func (t *Tracker) Snapshot() Snapshot {
	if !t.primed {
		return nil
	}
	return t.prev
}

// SettleFrom is the look after a turn: it compares issues with pre, the
// snapshot taken before the turn, records issues as the new baseline, and
// returns the edges the agent did not cause. New issues, reopens and severity
// changes that landed during the turn are reported; comment counts are not,
// because the agent comments on issues itself and the core does not say who
// wrote a comment.
func (t *Tracker) SettleFrom(pre Snapshot, issues []corecli.Issue) []Edge {
	cur := snapshot(issues)
	var out []Edge
	if pre != nil {
		out = diff(pre, cur, issues, false)
	}
	t.prev, t.primed = cur, true
	return out
}

// Rebaseline records issues as the new baseline without reporting edges.
func (t *Tracker) Rebaseline(issues []corecli.Issue) {
	t.prev, t.primed = snapshot(issues), true
}

func snapshot(issues []corecli.Issue) Snapshot {
	cur := make(Snapshot, len(issues))
	for _, is := range issues {
		cur[is.ID] = seen{is.Severity, is.ReopenCount, is.Comments}
	}
	return cur
}

func diff(prev, cur Snapshot, issues []corecli.Issue, comments bool) []Edge {
	var out []Edge
	for _, is := range issues {
		p, known := prev[is.ID]
		switch {
		case !known:
			out = append(out, Edge{Kind: New, IssueID: is.ID,
				Fact: fmt.Sprintf("new issue %s (%s): %s", Fence(is.ID, 80), Fence(is.Severity, 40), Fence(is.Summary, 160))})
			continue
		case is.ReopenCount > p.reopens:
			out = append(out, Edge{Kind: Reopened, IssueID: is.ID,
				Fact: fmt.Sprintf("issue %s was reopened (reopen count %d): %s", Fence(is.ID, 80), is.ReopenCount, Fence(is.Summary, 160))})
		}
		if is.Severity != p.severity {
			out = append(out, Edge{Kind: Severity, IssueID: is.ID,
				Fact: fmt.Sprintf("issue %s severity %s -> %s", Fence(is.ID, 80), Fence(p.severity, 40), Fence(is.Severity, 40))})
		}
		if comments && is.Comments > p.comments {
			out = append(out, Edge{Kind: Comment, IssueID: is.ID, Human: true,
				Fact: fmt.Sprintf("issue %s has %d new comment(s) since the last look (the core does not say who wrote them; read the issue)", Fence(is.ID, 80), is.Comments-p.comments)})
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].IssueID < out[j].IssueID })
	return out
}

// Fence renders text from outside (an issue summary, a name, a message) as a
// quoted string with no newline, cut on a rune boundary at n runes, so it
// cannot forge a line of the prompt.
func Fence(s string, n int) string {
	s = strings.TrimSpace(s)
	if n > 0 && utf8.RuneCountInString(s) > n {
		r := []rune(s)
		s = string(r[:n]) + "..."
	}
	return fmt.Sprintf("%q", s)
}
