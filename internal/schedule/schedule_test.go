package schedule

import (
	"strings"
	"testing"
	"time"
)

const (
	min = 2 * time.Minute
	max = 2 * time.Hour
)

func TestRequestClamps(t *testing.T) {
	cases := []struct {
		name  string
		after string
		want  time.Duration
		src   Source
	}{
		{"in range", "15m", 15 * time.Minute, SourceAgent},
		{"too short", "5s", min, SourceClamped},
		{"too long", "48h", max, SourceClamped},
		{"exactly min", "2m", min, SourceAgent},
		{"exactly max", "2h", max, SourceAgent},
		{"compound", "1h30m", 90 * time.Minute, SourceAgent},
		{"padded", " 10m ", 10 * time.Minute, SourceAgent},
		{"empty", "", min, SourceGarbled},
		{"bad duration", "soon", min, SourceGarbled},
		{"a bare number", "15", min, SourceGarbled},
		{"negative", "-5m", min, SourceGarbled},
		{"zero", "0s", min, SourceGarbled},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := Request(c.after, "why", min, max)
			if w.After != c.want || w.Source != c.src {
				t.Fatalf("got %s/%s (%s), want %s/%s", w.After, w.Source, w.Note, c.want, c.src)
			}
			if c.src != SourceAgent && w.Note == "" {
				t.Fatalf("a defaulted or clamped wake must explain itself")
			}
		})
	}
}

func TestWhyIsBounded(t *testing.T) {
	w := Request("10m", strings.Repeat("x", 5000), min, max)
	if len(w.Why) != maxWhy {
		t.Fatalf("why length %d, want %d", len(w.Why), maxWhy)
	}
}

func TestDefault(t *testing.T) {
	w := Default(min, SourceMissing, "no call")
	if w.After != min || w.Source != SourceMissing || w.Note != "no call" {
		t.Fatalf("%+v", w)
	}
}

func TestAfterTurnKeepsAnEarlierPromiseAndMakesAPersonsRequestOne(t *testing.T) {
	now := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	min, max := 10*time.Minute, 2*time.Hour

	// A person asks for a note in 3 minutes: not held to the floor, and a promise.
	p := AfterTurn(TurnEnd{Now: now, Min: min, Max: max, Human: true, Asked: true, After: "3m", Why: "the note"})
	if p.Wake.After != 3*time.Minute || p.Promise.At != now.Add(3*time.Minute) || !p.Promise.Human || p.Promise.Why != "the note" {
		t.Fatalf("a person's request: %+v", p)
	}

	// An edge wake that wants the idle schedule does not push the promise back.
	q := AfterTurn(TurnEnd{Now: now.Add(time.Minute), Min: min, Max: max, Asked: true, After: "1h", Why: "idle", Promise: p.Promise})
	if q.At != p.Promise.At || q.Why != "the note" || q.Wake.Note == "" || q.Promise != p.Promise {
		t.Fatalf("an earlier promise is kept: %+v", q)
	}

	// The promised wake itself clears it; the agent's own request is then not a human promise.
	r := AfterTurn(TurnEnd{Now: now.Add(3 * time.Minute), Min: min, Max: max, Tick: true, Asked: true, After: "1h", Why: "idle", Promise: p.Promise})
	if r.At != now.Add(3*time.Minute).Add(time.Hour) || r.Promise.Human {
		t.Fatalf("after the promised wake: %+v", r)
	}

	// No call: the minimum; a failure raises it to the backoff.
	f := AfterTurn(TurnEnd{Now: now, Min: min, Max: max, Failed: true, Backoff: 40 * time.Minute})
	if f.Wake.Source != SourceMissing || f.Wake.After != 40*time.Minute || !f.Promise.None() {
		t.Fatalf("a failed turn: %+v", f)
	}
}

func TestAfterShortTurnMovesThePromiseOnlyEarlier(t *testing.T) {
	now := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	cur := AfterShortTurn(now, 10*time.Minute, 2*time.Hour, true, "5m", "later", Promise{})
	if cur.At != now.Add(5*time.Minute) || !cur.Human {
		t.Fatalf("first: %+v", cur)
	}
	if got := AfterShortTurn(now, 10*time.Minute, 2*time.Hour, true, "30m", "much later", cur); got != cur {
		t.Fatalf("a later request moved the promise: %+v", got)
	}
	if got := AfterShortTurn(now, 10*time.Minute, 2*time.Hour, true, "2m", "sooner", cur); got.At != now.Add(2*time.Minute) {
		t.Fatalf("an earlier request was not taken: %+v", got)
	}
	if got := AfterShortTurn(now, 10*time.Minute, 2*time.Hour, false, "", "", cur); got != cur {
		t.Fatalf("no request changed it: %+v", got)
	}
}
