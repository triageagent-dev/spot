package schedule

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

const (
	min = 2 * time.Minute
	max = 2 * time.Hour
)

func TestParseClamps(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want time.Duration
		src  Source
	}{
		{"in range", `{"after":"15m","why":"check rollout"}`, 15 * time.Minute, SourceAgent},
		{"too short", `{"after":"5s","why":"spin"}`, min, SourceClamped},
		{"too long", `{"after":"48h","why":"sleep"}`, max, SourceClamped},
		{"exactly min", `{"after":"2m"}`, min, SourceAgent},
		{"exactly max", `{"after":"2h"}`, max, SourceAgent},
		{"compound", `{"after":"1h30m"}`, 90 * time.Minute, SourceAgent},
		{"not json", `after 15m`, min, SourceGarbled},
		{"empty", ``, min, SourceGarbled},
		{"missing after", `{"why":"x"}`, min, SourceGarbled},
		{"number", `{"after":15}`, min, SourceGarbled},
		{"bad duration", `{"after":"soon"}`, min, SourceGarbled},
		{"negative", `{"after":"-5m"}`, min, SourceGarbled},
		{"zero", `{"after":"0s"}`, min, SourceGarbled},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := Parse([]byte(c.in), min, max)
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
	long := make([]byte, 5000)
	for i := range long {
		long[i] = 'x'
	}
	w := Parse([]byte(`{"after":"10m","why":"`+string(long)+`"}`), min, max)
	if len(w.Why) != maxWhy {
		t.Fatalf("why length %d, want %d", len(w.Why), maxWhy)
	}
}

func TestConsumeRemovesFileAndHandlesMissing(t *testing.T) {
	p := filepath.Join(t.TempDir(), "next.json")
	if w := Consume(p, min, max); w.Source != SourceMissing || w.After != min {
		t.Fatalf("missing: %+v", w)
	}
	if err := os.WriteFile(p, []byte(`{"after":"7m","why":"w"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	w := Consume(p, min, max)
	if w.After != 7*time.Minute || w.Why != "w" {
		t.Fatalf("got %+v", w)
	}
	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Fatalf("next.json must be consumed, stat err = %v", err)
	}
	if w := Consume(p, min, max); w.Source != SourceMissing {
		t.Fatalf("second read must see it gone, got %+v", w)
	}
}
