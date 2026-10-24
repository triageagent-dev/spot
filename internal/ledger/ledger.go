// Package ledger keeps the supervisor's small durable state under /state: the
// session id the conversation resumes from, the daily token counter, the room
// cursors, and the status file the core and humans read. Writes are atomic.
//
// The state volume has two areas. The agent's area is the notes directory,
// next.json and the working directory; the agent may edit those. Everything
// the supervisor relies on lives in the supervisor directory, which the
// agent's Edit and Write tools are denied, and the counters that gate the
// agent (the daily usage) are held in memory and only written through, so a
// rewritten file changes nothing until the next restart.
package ledger

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Directories and files under the state directory.
const (
	// SupervisorDir holds the supervisor's own files. The agent cannot edit it.
	SupervisorDir = "supervisor"
	// NotesDir is the agent's notes; the agent reads and writes it freely.
	NotesDir = "notes"
	// WorkDir is the CLI's working directory: an empty directory of its own,
	// so the CLI picks up no project settings or instructions from the state
	// root.
	WorkDir = "work"
	// NextFile is the agent's schedule request, at the state root.
	NextFile = "next.json"

	SessionFile = "session.json"
	UsageFile   = "usage.json"
	StatusFile  = "status.json"
	RoomsFile   = "rooms.json"
)

// migrated are the files an earlier layout kept at the state root. New
// returns them to the supervisor directory once.
var migrated = []string{SessionFile, UsageFile, StatusFile}

// Ledger is safe for concurrent use.
type Ledger struct {
	dir string
	sup string
	now func() time.Time
	mu  sync.Mutex
	// usage is today's counter as the supervisor knows it. The file is a
	// write-through copy read once at start.
	usage Usage
}

// New returns a Ledger rooted at dir, creating the layout.
func New(dir string) (*Ledger, error) {
	sup := filepath.Join(dir, SupervisorDir)
	for _, d := range []string{dir, filepath.Join(dir, NotesDir), filepath.Join(dir, WorkDir)} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	if err := os.MkdirAll(sup, 0o700); err != nil {
		return nil, err
	}
	for _, name := range migrated {
		old, now := filepath.Join(dir, name), filepath.Join(sup, name)
		if _, err := os.Stat(old); err != nil {
			continue
		}
		if _, err := os.Stat(now); err == nil {
			continue
		}
		_ = os.Rename(old, now)
	}
	l := &Ledger{dir: dir, sup: sup, now: time.Now}
	_ = readJSON(l.Path(UsageFile), &l.usage)
	return l, nil
}

// SetClock replaces the clock, for tests.
func (l *Ledger) SetClock(f func() time.Time) { l.now = f }

// Dir is the state directory.
func (l *Ledger) Dir() string { return l.dir }

// Path joins name onto the supervisor directory.
func (l *Ledger) Path(name string) string { return filepath.Join(l.sup, name) }

// AgentPath joins name onto the state root, the agent's area.
func (l *Ledger) AgentPath(name string) string { return filepath.Join(l.dir, name) }

// Session is what a restart resumes from.
type Session struct {
	ID      string  `json:"id,omitempty"`
	Running bool    `json:"running,omitempty"` // a turn was in flight when this was written
	Since   string  `json:"since,omitempty"`   // when the id was first used
	CostUSD float64 `json:"costUsd,omitempty"` // last total_cost_usd the CLI reported for this conversation
}

// LoadSession returns the saved session; a missing or unreadable file is an
// empty Session.
func (l *Ledger) LoadSession() Session {
	var s Session
	l.mu.Lock()
	defer l.mu.Unlock()
	_ = readJSON(l.Path(SessionFile), &s)
	return s
}

// SaveSession persists s.
func (l *Ledger) SaveSession(s Session) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return writeJSON(l.Path(SessionFile), s, 0o600)
}

// Usage is the day's spend as the supervisor counts it.
type Usage struct {
	Date      string  `json:"date"` // UTC, 2006-01-02
	Tokens    int64   `json:"tokens"`
	CacheRead int64   `json:"cacheReadTokens"`
	Output    int64   `json:"outputTokens"`
	CostUSD   float64 `json:"costUsd"`
	Turns     int     `json:"turns"`
}

// Day formats t's UTC date the way Usage.Date does.
func Day(t time.Time) string { return t.UTC().Format("2006-01-02") }

// Usage returns today's counters; a counter from an earlier UTC day reads as
// zero. It never reads the file: the agent could have rewritten it.
func (l *Ledger) Usage() Usage {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.usageLocked()
}

func (l *Ledger) usageLocked() Usage {
	if today := Day(l.now()); l.usage.Date != today {
		return Usage{Date: today}
	}
	return l.usage
}

// AddUsage adds one turn's tokens (input + output + cache creation), cache
// reads, the output part of tokens on its own, and estimated cost to today's
// counter and returns the new totals.
func (l *Ledger) AddUsage(tokens, cacheRead, output int64, cost float64) (Usage, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	u := l.usageLocked()
	u.Tokens += tokens
	u.CacheRead += cacheRead
	u.Output += output
	u.CostUSD += cost
	u.Turns++
	l.usage = u
	return u, writeJSON(l.Path(UsageFile), u, 0o600)
}

// CeilingReached reports whether ceiling (> 0) is used up today.
func (l *Ledger) CeilingReached(ceiling int64) bool {
	return ceiling > 0 && l.Usage().Tokens >= ceiling
}

// CostCeilingReached reports whether ceiling dollars (> 0) are used up today.
func (l *Ledger) CostCeilingReached(ceiling float64) bool {
	return ceiling > 0 && l.Usage().CostUSD >= ceiling
}

// NextUTCMidnight is the first instant after now that is 00:00 UTC.
func NextUTCMidnight(now time.Time) time.Time {
	n := now.UTC()
	return time.Date(n.Year(), n.Month(), n.Day()+1, 0, 0, 0, 0, time.UTC)
}

// WriteStatus writes v as status.json (world-readable: it holds no secrets).
func (l *Ledger) WriteStatus(v any) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return writeJSON(l.Path(StatusFile), v, 0o644)
}

// LoadRoomCursors returns the saved room cursors (room name to cursor), so a
// restart carries on from the last message seen instead of dropping what was
// said while the pod was down.
func (l *Ledger) LoadRoomCursors() map[string]string {
	out := map[string]string{}
	l.mu.Lock()
	defer l.mu.Unlock()
	_ = readJSON(l.Path(RoomsFile), &out)
	return out
}

// SaveRoomCursors persists the room cursors.
func (l *Ledger) SaveRoomCursors(c map[string]string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return writeJSON(l.Path(RoomsFile), c, 0o600)
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

func writeJSON(path string, v any, mode os.FileMode) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err := tmp.Chmod(mode); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(append(b, '\n')); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// IsNotExist reports a missing state file.
func IsNotExist(err error) bool { return errors.Is(err, os.ErrNotExist) }
