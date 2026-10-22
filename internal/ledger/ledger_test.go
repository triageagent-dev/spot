package ledger

import (
	"os"
	"testing"
	"time"
)

func newL(t *testing.T, now time.Time) *Ledger {
	t.Helper()
	l, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	l.SetClock(func() time.Time { return now })
	return l
}

func TestUsageAccumulatesAndSurvivesRestart(t *testing.T) {
	now := time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC)
	l := newL(t, now)
	if _, err := l.AddUsage(1000, 50, 0.1); err != nil {
		t.Fatal(err)
	}
	if _, err := l.AddUsage(500, 0, 0.05); err != nil {
		t.Fatal(err)
	}
	// A new Ledger over the same directory is a restart.
	l2, _ := New(l.Dir())
	l2.SetClock(func() time.Time { return now })
	u := l2.Usage()
	if u.Tokens != 1500 || u.CacheRead != 50 || u.Turns != 2 {
		t.Fatalf("usage after restart: %+v", u)
	}
}

func TestCeilingAndUTCRollover(t *testing.T) {
	day1 := time.Date(2026, 10, 21, 23, 59, 0, 0, time.UTC)
	l := newL(t, day1)
	if _, err := l.AddUsage(900, 0, 0); err != nil {
		t.Fatal(err)
	}
	if l.CeilingReached(1000) {
		t.Fatal("900 of 1000 is not a reached ceiling")
	}
	if _, err := l.AddUsage(100, 0, 0); err != nil {
		t.Fatal(err)
	}
	if !l.CeilingReached(1000) {
		t.Fatal("1000 of 1000 must be reached")
	}
	if l.CeilingReached(0) {
		t.Fatal("ceiling 0 means no ceiling")
	}
	if got := NextUTCMidnight(day1); !got.Equal(time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("next midnight %v", got)
	}
	l.SetClock(func() time.Time { return day1.Add(2 * time.Minute) })
	if l.CeilingReached(1000) || l.Usage().Tokens != 0 {
		t.Fatalf("a new UTC day must reset the counter: %+v", l.Usage())
	}
}

func TestSessionRoundTripAndStatusMode(t *testing.T) {
	l := newL(t, time.Now())
	if s := l.LoadSession(); s.ID != "" {
		t.Fatalf("fresh session: %+v", s)
	}
	if err := l.SaveSession(Session{ID: "abc", Running: true}); err != nil {
		t.Fatal(err)
	}
	if s := l.LoadSession(); s.ID != "abc" || !s.Running {
		t.Fatalf("session: %+v", s)
	}
	if err := l.WriteStatus(map[string]string{"state": "idle"}); err != nil {
		t.Fatal(err)
	}
	st, _ := os.Stat(l.Path(StatusFile))
	if st.Mode().Perm() != 0o644 {
		t.Fatalf("status mode %o", st.Mode().Perm())
	}
}

func TestCostCeiling(t *testing.T) {
	l := newL(t, time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC))
	if l.CostCeilingReached(0) || l.CostCeilingReached(5) {
		t.Fatal("nothing spent yet, and 0 means no ceiling")
	}
	if _, err := l.AddUsage(100, 0, 3.5); err != nil {
		t.Fatal(err)
	}
	if l.CostCeilingReached(5) || !l.CostCeilingReached(3.5) || l.CostCeilingReached(0) {
		t.Fatalf("usage %+v", l.Usage())
	}
}

func TestUsageIsHeldInMemoryNotReadFromTheFile(t *testing.T) {
	now := time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC)
	l := newL(t, now)
	if _, err := l.AddUsage(100, 0, 0); err != nil {
		t.Fatal(err)
	}
	// The agent rewrites the ledger file: nothing changes until a restart,
	// and a restart reads back what was last written through.
	if err := os.WriteFile(l.Path(UsageFile), []byte(`{"date":"2026-10-21","tokens":0}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if u := l.Usage(); u.Tokens != 100 {
		t.Fatalf("a rewritten file changed the in-memory count: %+v", u)
	}
	if _, err := l.AddUsage(1, 0, 0); err != nil {
		t.Fatal(err)
	}
	again, err := New(l.Dir())
	if err != nil {
		t.Fatal(err)
	}
	again.SetClock(func() time.Time { return now })
	if u := again.Usage(); u.Tokens != 101 {
		t.Fatalf("restart must read the written-through count: %+v", u)
	}
}

func TestLayoutAndMigration(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(dir+"/"+UsageFile, []byte(`{"date":"2026-10-21","tokens":7,"turns":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	l, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	l.SetClock(func() time.Time { return time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC) })
	if u := l.Usage(); u.Tokens != 7 {
		t.Fatalf("the old layout's ledger was not migrated: %+v", u)
	}
	if _, err := os.Stat(dir + "/" + UsageFile); !os.IsNotExist(err) {
		t.Fatal("the old file must be gone from the agent's area")
	}
	for _, d := range []string{NotesDir, WorkDir, SupervisorDir} {
		if st, err := os.Stat(dir + "/" + d); err != nil || !st.IsDir() {
			t.Fatalf("%s missing", d)
		}
	}
	if l.AgentPath(NextFile) != dir+"/"+NextFile || l.Path(StatusFile) != dir+"/"+SupervisorDir+"/"+StatusFile {
		t.Fatalf("paths: %s %s", l.AgentPath(NextFile), l.Path(StatusFile))
	}
}

func TestRoomCursorsRoundTrip(t *testing.T) {
	l := newL(t, time.Now())
	if c := l.LoadRoomCursors(); len(c) != 0 {
		t.Fatalf("fresh cursors: %v", c)
	}
	if err := l.SaveRoomCursors(map[string]string{"team": "1761000000000"}); err != nil {
		t.Fatal(err)
	}
	if c := l.LoadRoomCursors(); c["team"] != "1761000000000" {
		t.Fatalf("cursors: %v", c)
	}
}
