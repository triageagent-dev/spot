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
