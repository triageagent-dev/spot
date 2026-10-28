package main

import (
	"context"
	"time"

	"github.com/triageagent-dev/spot/internal/durable"
	"github.com/triageagent-dev/spot/internal/triggers"
)

// controlOf answers the core's controls with the temporal engine: the core names
// an operation and a task key, the engine checks the key against what the spot
// has in hand and acts on that workflow only.
func controlOf(eng *durable.Engine) triggers.ControlFunc {
	return func(ctx context.Context, c triggers.Control) triggers.ControlResult {
		r := eng.Control(ctx, c.Op, c.Task)
		out := triggers.ControlResult{ID: c.ID, OK: r.OK, Msg: r.Msg}
		now := time.Now()
		for _, w := range r.Work {
			l := triggers.TaskLine{Key: w.Key, Title: w.Title, Lane: w.Lane, State: w.State}
			if !w.Started.IsZero() {
				l.StartedMs, l.AgeMs = w.Started.UnixMilli(), now.Sub(w.Started).Milliseconds()
			}
			out.Tasks = append(out.Tasks, l)
		}
		return out
	}
}
