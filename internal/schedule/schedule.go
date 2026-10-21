// Package schedule reads the agent's own wake-up request, /state/next.json,
// and clamps it so a confused agent can neither spin nor sleep forever.
package schedule

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

// Source says where a Wake came from.
type Source string

const (
	SourceAgent   Source = "agent"           // read as written
	SourceClamped Source = "agent-clamped"   // read, then moved into [min,max]
	SourceMissing Source = "default-missing" // no file: min-wake
	SourceGarbled Source = "default-garbled" // unreadable or invalid: min-wake
)

// Wake is the delay and reason for the next scheduled turn.
type Wake struct {
	After  time.Duration
	Why    string
	Source Source
	Note   string // why it was defaulted or clamped, for the log
}

type file struct {
	After json.RawMessage `json:"after"`
	Why   string          `json:"why"`
}

// maxWhy bounds the reason carried into prompts, the status file and the
// heartbeat.
const maxWhy = 400

// Parse turns the bytes of next.json into a Wake clamped to [min,max]. It
// never fails: a bad file yields min with SourceGarbled.
func Parse(b []byte, min, max time.Duration) Wake {
	var f file
	if err := json.Unmarshal(b, &f); err != nil {
		return Wake{After: min, Why: "", Source: SourceGarbled, Note: "next.json is not valid JSON: " + err.Error()}
	}
	after, err := parseAfter(f.After)
	if err != nil {
		return Wake{After: min, Why: clip(f.Why), Source: SourceGarbled, Note: err.Error()}
	}
	w := Wake{After: after, Why: clip(f.Why), Source: SourceAgent}
	switch {
	case after < min:
		w.After, w.Source, w.Note = min, SourceClamped, fmt.Sprintf("requested %s, raised to min-wake %s", after, min)
	case after > max:
		w.After, w.Source, w.Note = max, SourceClamped, fmt.Sprintf("requested %s, lowered to max-wake %s", after, max)
	}
	return w
}

// Default is the wake used when there is no usable next.json.
func Default(min time.Duration, src Source, note string) Wake {
	return Wake{After: min, Source: src, Note: note}
}

// Consume reads path, removes it so a stale request cannot be read twice, and
// returns the clamped Wake. A missing file is SourceMissing.
func Consume(path string, min, max time.Duration) Wake {
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Default(min, SourceMissing, "agent wrote no next.json")
		}
		return Default(min, SourceGarbled, "cannot read next.json: "+err.Error())
	}
	_ = os.Remove(path)
	return Parse(b, min, max)
}

func parseAfter(raw json.RawMessage) (time.Duration, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, errors.New(`next.json has no "after"`)
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return 0, fmt.Errorf(`"after" must be a duration string such as "15m": %s`, strings.TrimSpace(string(raw)))
	}
	d, err := time.ParseDuration(strings.TrimSpace(s))
	if err != nil {
		return 0, fmt.Errorf(`"after" %q is not a duration: %v`, s, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf(`"after" %q is not positive`, s)
	}
	return d, nil
}

func clip(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > maxWhy {
		s = s[:maxWhy]
	}
	return s
}
