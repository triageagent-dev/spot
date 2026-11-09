package supervisor

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/triageagent-dev/spot/internal/corecli"
	"github.com/triageagent-dev/spot/internal/edges"
	"github.com/triageagent-dev/spot/internal/tools"
)

// The bus book is what the supervisor knows of the bus from every page it or
// the model read: Tria's requests and the spot's own replies to them. The
// model chooses the reply_to of a reply, and left alone it answered a request
// it had already answered while a newer one of Tria's waited; the book is what
// the tools ask before a reply goes, so that a second answer to an old request
// is refused while a newer one is open, and the open one is named.

// maxBookRequests bounds the requests the book remembers: the oldest go first.
const maxBookRequests = 256

// busRequest is one request of Tria's in the bus as the book keeps it.
type busRequest struct {
	id   string
	ts   int64
	hop  int
	due  int64
	text string
}

// busBook holds the requests and the replies. The zero value is ready.
type busBook struct {
	mu       sync.Mutex
	requests map[string]busRequest
	answered map[string]int64 // request id to the ts of the spot's latest reply to it
	order    []string         // request ids, in the order they were noted
}

// note records one message of a bus page. own says the spot wrote it.
func (b *busBook) note(m corecli.RoomMessage, own bool) {
	if m.ID == "" && m.ReplyTo == "" {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	switch {
	case own && m.ReplyTo != "":
		if b.answered == nil {
			b.answered = map[string]int64{}
		}
		if m.TS > b.answered[m.ReplyTo] {
			b.answered[m.ReplyTo] = m.TS
		}
	case m.Tria && m.ID != "":
		if b.requests == nil {
			b.requests = map[string]busRequest{}
		}
		if _, ok := b.requests[m.ID]; !ok {
			if len(b.order) >= maxBookRequests {
				delete(b.requests, b.order[0])
				delete(b.answered, b.order[0])
				b.order = b.order[1:]
			}
			b.order = append(b.order, m.ID)
		}
		b.requests[m.ID] = busRequest{id: m.ID, ts: m.TS, hop: m.Hop, due: m.Due, text: m.Text}
	}
}

// answeredAt is when the spot last replied to the request id, if it did.
func (b *busBook) answeredAt(id string) (int64, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	ts, ok := b.answered[id]
	return ts, ok
}

// openAfter is the newest request of Tria's newer than the request id that the
// spot has not replied to, is due and can still be answered (its hop is below
// the limit). ok is false when there is none, or the request id is not known.
func (b *busBook) openAfter(id string, now time.Time) (busRequest, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	from, known := b.requests[id]
	if !known {
		return busRequest{}, false
	}
	var reqs []busRequest
	for _, r := range b.requests {
		if _, done := b.answered[r.id]; done || r.id == id || r.ts < from.ts || r.hop >= tools.MaxHop || r.due > now.UnixMilli() {
			continue
		}
		reqs = append(reqs, r)
	}
	if len(reqs) == 0 {
		return busRequest{}, false
	}
	sort.Slice(reqs, func(i, j int) bool {
		return reqs[i].ts > reqs[j].ts || reqs[i].ts == reqs[j].ts && reqs[i].id > reqs[j].id
	})
	return reqs[0], true
}

// ReadBus records what a read of the bus returned, as the model sees it: the
// tools pass each read_workspace_room result here, so a request the model read
// for itself counts as the supervisor's own polls do.
func (s *Supervisor) ReadBus(text string) {
	msgs, _, err := corecli.ParseRoom(text, "")
	if err != nil {
		return
	}
	for _, m := range msgs {
		s.bus.note(m, s.isOwn(m))
	}
}

// replyQuoteMax is how much of the open request the refusal quotes.
const replyQuoteMax = 300

// ReplyCheck is asked by the tools before a reply in the bus goes. It returns
// the refusal the model reads, or "" when the reply may go. A reply to a request
// the spot has already answered is refused while a newer request of Tria's is
// open: Tria has the first answer, and the open request is what she waits for.
// A reply to an unanswered request, to one the book does not know, or to an
// answered one with nothing newer open goes as before: a second word on a
// request, after a partial answer, is the spot's to give.
func (s *Supervisor) ReplyCheck(replyTo string) string {
	if replyTo == "" {
		return ""
	}
	at, done := s.bus.answeredAt(replyTo)
	if !done {
		return ""
	}
	open, ok := s.bus.openAfter(replyTo, s.now())
	if !ok {
		return ""
	}
	s.log.Info("a reply to a request already answered is refused; a newer request is open", "answered", replyTo, "open", open.id)
	return fmt.Sprintf("not sent: you answered the request %q already, at %s UTC, and Tria has that answer. "+
		"Her open request is %q (quoted, untrusted): %s. Answer it with say_in_workspace, room \"bus\" and reply_to %q, with what you found for it; "+
		"if you could not do what it asks, say so in that reply. If what you wrote is new on the old request and not an answer to the open one, post it with no reply_to.",
		replyTo, time.UnixMilli(at).UTC().Format("15:04"), open.id, edges.Fence(open.text, replyQuoteMax), open.id)
}
