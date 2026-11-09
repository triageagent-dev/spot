package supervisor

import (
	"context"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/triageagent-dev/spot/internal/corecli"
	"github.com/triageagent-dev/spot/internal/ledger"
)

// bookSupervisor is a supervisor with no loop running, at a fixed time.
func bookSupervisor(t *testing.T) (*Supervisor, *harness) {
	t.Helper()
	h := newHarness(t, nil)
	led, err := ledger.New(h.state)
	must(t, err)
	s := New(h.cfg, led, h.core, h.script, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.now = h.clock.Now
	return s, h
}

func triaAsks(id, text string, ts int64, hop int) corecli.RoomMessage {
	return corecli.RoomMessage{ID: id, Sender: "@tria:srv", Name: "Tria", Text: text, TS: ts, Tria: true, Hop: hop}
}

func spotReplies(id, to string, ts int64) corecli.RoomMessage {
	return corecli.RoomMessage{ID: id, Sender: "@u.spot.example.example-spot:srv", Spot: "example-spot", Text: "done", TS: ts, ReplyTo: to, Hop: 2}
}

// A second answer to a request the spot already answered is refused while a
// newer request of Tria's waits, and the refusal names the open one. The
// open request itself, a request the book does not know, and a request with
// nothing newer open are answered as before.
func TestASecondAnswerIsRefusedWhileANewerRequestIsOpen(t *testing.T) {
	s, _ := bookSupervisor(t)
	now := s.now().UnixMilli()
	for _, m := range []corecli.RoomMessage{
		triaAsks("$flux", "is auth managed by Flux?", now-60_000, 1),
		spotReplies("$a1", "$flux", now-50_000),
		triaAsks("$opa", "find the memory bottleneck in the OPA container", now-30_000, 1),
	} {
		s.bus.note(m, s.isOwn(m))
	}
	got := s.ReplyCheck("$flux")
	if !strings.Contains(got, "not sent") || !strings.Contains(got, `"$opa"`) || !strings.Contains(got, "memory bottleneck") {
		t.Fatalf("a second answer to the old request while the new one waits:\n%s", got)
	}
	if got := s.ReplyCheck("$opa"); got != "" {
		t.Errorf("the open request is answered: %q", got)
	}
	if got := s.ReplyCheck("$unknown"); !strings.Contains(got, `"$opa"`) {
		t.Errorf("a reply to a message the book does not know while her request waits: %q", got)
	}
	if got := s.ReplyCheck(""); got != "" {
		t.Errorf("a message that answers nothing is not asked about: %q", got)
	}
	// Once the open request is answered, a second word on the old one goes:
	// after a partial answer, the rest is the spot's to give. So does a reply
	// to a message the book does not know.
	s.bus.note(spotReplies("$a2", "$opa", now-10_000), true)
	if got := s.ReplyCheck("$flux"); got != "" {
		t.Errorf("nothing newer open, the reply goes: %q", got)
	}
	if got := s.ReplyCheck("$unknown"); got != "" {
		t.Errorf("nothing of hers open, a reply the book cannot judge goes: %q", got)
	}
}

// While a request of Tria's is open, a reply threaded to the spot's own
// message, or to a message the book does not know as hers, is refused with
// the open request named; with nothing of hers open such a reply goes.
func TestAReplyToAMessageThatIsNotHersIsRefusedWhileARequestIsOpen(t *testing.T) {
	s, _ := bookSupervisor(t)
	now := s.now().UnixMilli()
	own := corecli.RoomMessage{ID: "$mine", Sender: "@u.spot.example.example-spot:srv", Spot: "example-spot", Text: "is the hold lifted?", TS: now - 90_000, Hop: 1}
	s.bus.note(own, true)
	if got := s.ReplyCheck("$mine"); got != "" {
		t.Errorf("nothing of Tria's open, a reply to the spot's own message goes: %q", got)
	}
	s.bus.note(triaAsks("$opa", "find the memory bottleneck", now-30_000, 1), false)
	got := s.ReplyCheck("$mine")
	if !strings.Contains(got, "your own message") || !strings.Contains(got, `"$opa"`) {
		t.Errorf("a reply to the spot's own message while a request waits:\n%s", got)
	}
	got = s.ReplyCheck("$somewhere")
	if !strings.Contains(got, "not one of Tria's requests") || !strings.Contains(got, `"$opa"`) {
		t.Errorf("a reply to a message the book does not know while a request waits:\n%s", got)
	}
	if got := s.ReplyCheck("$opa"); got != "" {
		t.Errorf("the open request is answered: %q", got)
	}
	s.bus.note(spotReplies("$a1", "$opa", now-10_000), true)
	if got := s.ReplyCheck("$mine"); got != "" {
		t.Errorf("once her request is answered, a word on the spot's own message goes: %q", got)
	}
}

// A request the spot cannot answer (at the hop limit) or that is not due yet
// is not an open one: it does not hold back a word on an older request.
func TestARequestNotYetAnswerableIsNotOpen(t *testing.T) {
	s, _ := bookSupervisor(t)
	now := s.now().UnixMilli()
	s.bus.note(triaAsks("$old", "first", now-60_000, 1), false)
	s.bus.note(spotReplies("$a1", "$old", now-50_000), true)
	s.bus.note(triaAsks("$deep", "a follow-up at the limit", now-30_000, 3), false)
	later := triaAsks("$later", "tomorrow", now-20_000, 1)
	later.Due = now + int64(time.Hour/time.Millisecond)
	s.bus.note(later, false)
	if got := s.ReplyCheck("$old"); got != "" {
		t.Errorf("a request at the hop limit or not due yet holds nothing back: %q", got)
	}
	s.bus.note(triaAsks("$new", "now", now-10_000, 1), false)
	if got := s.ReplyCheck("$old"); !strings.Contains(got, `"$new"`) {
		t.Errorf("the due request at hop 1 is the open one: %q", got)
	}
}

// What the model reads of the bus for itself, and what the supervisor polls,
// both fill the book: the spot's own replies by their spot name, Tria's
// requests by the tria mark.
func TestTheBookReadsThePollsAndTheModelsReads(t *testing.T) {
	s, h := bookSupervisor(t)
	now := s.now().UnixMilli()
	h.core.roomMsgs["bus"] = []corecli.RoomMessage{
		triaAsks("$flux", "is auth managed by Flux?", now-60_000, 1),
		spotReplies("$a1", "$flux", now-50_000),
	}
	s.pollRoom(context.Background(), BusRoom)
	if got := s.ReplyCheck("$flux"); got != "" {
		t.Fatalf("answered, nothing newer: %q", got)
	}
	s.ReadBus(`{"messages":[{"id":"$opa","sender":"@tria:srv","name":"Tria","body":"find the memory bottleneck","ts":` + itoa(now-30_000) + `,"tria":true,"hop":1}],"next":"` + itoa(now-30_000) + `"}`)
	if got := s.ReplyCheck("$flux"); !strings.Contains(got, `"$opa"`) {
		t.Errorf("a request the model read is open: %q", got)
	}
	s.ReadBus(`{"messages":[{"id":"$a2","sender":"@u.spot.example.example-spot:srv","spot":"example-spot","body":"found it","ts":` + itoa(now-5_000) + `,"replyTo":"$opa","hop":2}]}`)
	if got := s.ReplyCheck("$flux"); got != "" {
		t.Errorf("the model's own reply it read back closes the request: %q", got)
	}
	s.ReadBus("not json at all")
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }
