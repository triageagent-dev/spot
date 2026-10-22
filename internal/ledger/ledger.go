// Package ledger keeps the supervisor's small durable state under /state: the
// session id the conversation resumes from, the daily token counter, and the
// status file the core and humans read. Writes are atomic.
package ledger

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// File names under the state directory.
const (
	SessionFile = "session.json"
	UsageFile   = "usage.json"
	StatusFile  = "status.json"
	NextFile    = "next.json"
	NotesDir    = "notes"
	AuditFile   = "audit.log"
)

// Ledger is safe for concurrent use.
type Ledger struct {
	dir string
	now func() time.Time
	mu  sync.Mutex
}

// New returns a Ledger rooted at dir, creating it and the notes directory.
func New(dir string) (*Ledger, error) {
	for _, d := range []string{dir, filepath.Join(dir, NotesDir)} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	return &Ledger{dir: dir, now: time.Now}, nil
}

// SetClock replaces the clock, for tests.
func (l *Ledger) SetClock(f func() time.Time) { l.now = f }

// Dir is the state directory.
func (l *Ledger) Dir() string { return l.dir }

// Path joins name onto the state directory.
func (l *Ledger) Path(name string) string { return filepath.Join(l.dir, name) }

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
	CostUSD   float64 `json:"costUsd"`
	Turns     int     `json:"turns"`
}

// Day formats t's UTC date the way Usage.Date does.
func Day(t time.Time) string { return t.UTC().Format("2006-01-02") }

// Usage returns today's counters; a counter from an earlier UTC day reads as
// zero.
func (l *Ledger) Usage() Usage {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.usageLocked()
}

func (l *Ledger) usageLocked() Usage {
	var u Usage
	_ = readJSON(l.Path(UsageFile), &u)
	if today := Day(l.now()); u.Date != today {
		return Usage{Date: today}
	}
	return u
}

// AddUsage adds one turn's tokens (input + output + cache creation), cache
// reads and estimated cost to today's counter and returns the new totals.
func (l *Ledger) AddUsage(tokens, cacheRead int64, cost float64) (Usage, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	u := l.usageLocked()
	u.Tokens += tokens
	u.CacheRead += cacheRead
	u.CostUSD += cost
	u.Turns++
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
