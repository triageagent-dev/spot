// Package ledger keeps the supervisor's small durable state under the state
// directory: whether a turn was in flight, the daily token counter, the room
// cursors, and the status file the core and humans read. Writes are atomic.
// All of it can be lost without harm: the spot starts fresh from the core's
// memory, and the counter resets at UTC midnight anyway.
//
// The model has no filesystem tool and no shell, so nothing here is its to
// edit; the counters that gate it are still held in memory and only written
// through, so a rewritten file changes nothing until the next restart.
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
	// SupervisorDir holds the supervisor's own files.
	SupervisorDir = "supervisor"

	SessionFile  = "session.json"
	UsageFile    = "usage.json"
	StatusFile   = "status.json"
	RoomsFile    = "rooms.json"
	DigestFile   = "digest.json"
	PromiseFile  = "promise.json"
	WindowFile   = "window.json"
	RoomReadFile = "roomread.json"
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
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
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

// Session is what a restart knows of the last turn. No conversation is kept:
// every wake starts an empty one.
type Session struct {
	Running bool `json:"running,omitempty"` // a turn was in flight when this was written
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
	Date      string `json:"date"` // UTC, 2006-01-02
	Tokens    int64  `json:"tokens"`
	CacheRead int64  `json:"cacheReadTokens"`
	Output    int64  `json:"outputTokens"`
	Turns     int    `json:"turns"`
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
// reads and the output part of tokens on its own to today's counter and
// returns the new totals.
func (l *Ledger) AddUsage(tokens, cacheRead, output int64) (Usage, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	u := l.usageLocked()
	u.Tokens += tokens
	u.CacheRead += cacheRead
	u.Output += output
	u.Turns++
	l.usage = u
	return u, writeJSON(l.Path(UsageFile), u, 0o600)
}

// RaiseTokens lifts today's token counter to tokens when it is lower, and says
// whether it did: the core's count of this spot's turns is the floor, so a lost
// or rewritten state directory cannot give the day's budget back.
func (l *Ledger) RaiseTokens(tokens int64) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	u := l.usageLocked()
	if tokens <= u.Tokens {
		return false, nil
	}
	u.Tokens = tokens
	l.usage = u
	return true, writeJSON(l.Path(UsageFile), u, 0o600)
}

// CeilingReached reports whether ceiling (> 0) is used up today.
func (l *Ledger) CeilingReached(ceiling int64) bool {
	return ceiling > 0 && l.Usage().Tokens >= ceiling
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

// LoadRoomRead returns, per room, the timestamp (ms) of the newest message the
// last finished turn could have seen.
func (l *Ledger) LoadRoomRead() map[string]int64 {
	out := map[string]int64{}
	l.mu.Lock()
	defer l.mu.Unlock()
	_ = readJSON(l.Path(RoomReadFile), &out)
	return out
}

// SaveRoomRead persists it.
func (l *Ledger) SaveRoomRead(m map[string]int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return writeJSON(l.Path(RoomReadFile), m, 0o600)
}

// DigestEntry is what one wake left for the next: the spot's own summary of it.
type DigestEntry struct {
	At      string `json:"at"`
	Reason  string `json:"reason"`
	Summary string `json:"summary"`
	Next    string `json:"next,omitempty"` // why it scheduled the next wake
}

// LoadDigest returns the saved digest of the last wakes, oldest first; a
// missing file is an empty digest.
func (l *Ledger) LoadDigest() []DigestEntry {
	var out []DigestEntry
	l.mu.Lock()
	defer l.mu.Unlock()
	_ = readJSON(l.Path(DigestFile), &out)
	return out
}

// SaveDigest persists the digest.
func (l *Ledger) SaveDigest(d []DigestEntry) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return writeJSON(l.Path(DigestFile), d, 0o600)
}

// Promise is a wake the agent asked for on a person's request ("in 3
// minutes"): it must come even when the pod restarts or a hold rule is active.
type Promise struct {
	At  time.Time `json:"at"`
	Why string    `json:"why"`
}

// LoadPromise returns the saved promised wake; a missing file is the zero
// Promise.
func (l *Ledger) LoadPromise() Promise {
	var p Promise
	l.mu.Lock()
	defer l.mu.Unlock()
	_ = readJSON(l.Path(PromiseFile), &p)
	return p
}

// SavePromise persists the promised wake; the zero Promise clears it.
func (l *Ledger) SavePromise(p Promise) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return writeJSON(l.Path(PromiseFile), p, 0o600)
}

// Window is the state a spot that is suspended between wakes would lose: the
// tokens each turn billed in the last hour (the hourly ceiling), when the early
// wakes ran (their cap), and the issue state the edges are told against. It is
// written when the spot goes idle.
type Window struct {
	Recent    []WindowSample `json:"recent,omitempty"`
	EdgeWakes []time.Time    `json:"edgeWakes,omitempty"`
	// Edges is the tracker's baseline, kept as the edges package writes it.
	Edges json.RawMessage `json:"edges,omitempty"`
}

// WindowSample is the tokens a turn billed, and when.
type WindowSample struct {
	At     time.Time `json:"at"`
	Tokens int64     `json:"tokens"`
}

// LoadWindow returns the saved window; a missing file is an empty one.
func (l *Ledger) LoadWindow() Window {
	var w Window
	l.mu.Lock()
	defer l.mu.Unlock()
	_ = readJSON(l.Path(WindowFile), &w)
	return w
}

// SaveWindow persists the window.
func (l *Ledger) SaveWindow(w Window) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return writeJSON(l.Path(WindowFile), w, 0o600)
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
	defer func() { _ = os.Remove(tmp.Name()) }()
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(append(b, '\n')); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// IsNotExist reports a missing state file.
func IsNotExist(err error) bool { return errors.Is(err, os.ErrNotExist) }
