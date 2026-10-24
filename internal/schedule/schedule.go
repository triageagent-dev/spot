// Package schedule turns the agent's own wake-up request (its schedule_next
// call) into a wake time, clamped so a confused agent can neither spin nor
// sleep forever.
package schedule

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Source says where a Wake came from.
type Source string

const (
	SourceAgent   Source = "agent"           // taken as asked
	SourceClamped Source = "agent-clamped"   // asked for, then moved into [min,max]
	SourceMissing Source = "default-missing" // no request: min-wake
	SourceGarbled Source = "default-garbled" // unusable request: min-wake
)

// Wake is the delay and reason for the next scheduled turn.
type Wake struct {
	After  time.Duration
	Why    string
	Source Source
	Note   string // why it was defaulted or clamped, for the log
}

// maxWhy bounds the reason carried into prompts, the status file and the
// heartbeat.
const maxWhy = 400

// Request turns what the agent asked for (a Go duration string and a reason)
// into a Wake clamped to [min,max]. It never fails: an unusable request yields
// min with SourceGarbled.
func Request(after, why string, min, max time.Duration) Wake {
	d, err := parseAfter(after)
	if err != nil {
		return Wake{After: min, Why: clip(why), Source: SourceGarbled, Note: err.Error()}
	}
	w := Wake{After: d, Why: clip(why), Source: SourceAgent}
	switch {
	case d < min:
		w.After, w.Source, w.Note = min, SourceClamped, fmt.Sprintf("requested %s, raised to min-wake %s", d, min)
	case d > max:
		w.After, w.Source, w.Note = max, SourceClamped, fmt.Sprintf("requested %s, lowered to max-wake %s", d, max)
	}
	return w
}

// Default is the wake used when the agent made no usable request.
func Default(min time.Duration, src Source, note string) Wake {
	return Wake{After: min, Source: src, Note: note}
}

func parseAfter(s string) (time.Duration, error) {
	if strings.TrimSpace(s) == "" {
		return 0, errors.New(`schedule_next has no "after"`)
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
