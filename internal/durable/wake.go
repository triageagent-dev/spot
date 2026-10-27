package durable

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/triageagent-dev/spot/internal/agent"
	"github.com/triageagent-dev/spot/internal/llm"
	"github.com/triageagent-dev/spot/internal/tools"
)

// discard is the logger the loop gets inside a workflow: a workflow runs again
// from its history, and a log line written there would be written every time.
// What the loop would have logged is in the WakeOut flags, and the process logs
// it when it books the turn.
var discard = slog.New(slog.DiscardHandler)

// WakeWorkflow is one turn of the agent loop. Each model request and each tool
// call is an activity, so a crash resumes the turn at its step; what the model
// has said and seen is the workflow's history. A call that may have an effect is
// run once: if its worker dies the model is told it may or may not have run.
func WakeWorkflow(ctx workflow.Context, in WakeIn) (WakeOut, error) {
	start := workflow.Now(ctx)
	turnCtx, cancelTurn := workflow.WithCancel(ctx)
	defer cancelTurn()

	// The turn's own time: the loop sees it as a stop, like a shutdown.
	timedOut := false
	timerCtx, cancelTimer := workflow.WithCancel(ctx)
	defer cancelTimer()
	if in.Turn > 0 {
		timer := workflow.NewTimer(timerCtx, in.Turn)
		workflow.Go(ctx, func(ctx workflow.Context) {
			if timer.Get(ctx, nil) == nil {
				timedOut = true
				cancelTurn()
			}
		})
	}

	var out WakeOut
	finish := func(text, errMsg string) {
		// Close the tools and the trace even when the turn was cancelled.
		dctx, _ := workflow.NewDisconnectedContext(ctx)
		dctx = workflow.WithActivityOptions(dctx, workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second, RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 2},
		})
		_ = workflow.ExecuteActivity(dctx, (*Acts).EndWake, EndIn{Key: in.Key, Text: text, Err: errMsg}).Get(dctx, nil)
	}

	// Typing runs beside the turn and ends with it. The workflow renews it with
	// short activities, so ending it never waits for one to notice a cancel.
	typingOpts := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second, RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	typingStop, typingRunning := workflow.WithCancel(ctx)
	typingEnded := workflow.NewBufferedChannel(ctx, 1)
	if in.Typing {
		workflow.Go(ctx, func(ctx workflow.Context) {
			defer typingEnded.SendAsync(true)
			for typingStop.Err() == nil {
				var shown bool
				if err := workflow.ExecuteActivity(typingOpts, (*Acts).Typing, TypingIn{Room: in.Room, On: true}).Get(ctx, &shown); err != nil || !shown {
					return
				}
				if workflow.NewTimer(typingStop, typingEvery).Get(ctx, nil) != nil {
					return
				}
			}
		})
	}
	endTyping := func() {
		if !in.Typing {
			return
		}
		typingRunning()
		typingEnded.Receive(ctx, nil)
		dctx, _ := workflow.NewDisconnectedContext(ctx)
		_ = workflow.ExecuteActivity(workflow.WithActivityOptions(dctx, workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second, RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 1},
		}), (*Acts).Typing, TypingIn{Room: in.Room, On: false}).Get(dctx, nil)
	}

	var lo ListOut
	err := workflow.ExecuteActivity(workflow.WithActivityOptions(turnCtx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute, RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 2, InitialInterval: time.Second},
	}), (*Acts).ListTools, ListIn{Key: in.Key, Reason: in.Reason, Prompt: in.Prompt, Limited: in.Limited, SpotID: in.SpotID, SpotRun: in.SpotRun}).Get(ctx, &lo)
	switch {
	case err != nil:
		out.Failed, out.Err = true, "the tools could not be listed: "+errText(err)
		endTyping()
		finish("", out.Err)
		return out, nil
	case lo.CoreDown:
		out.Failed, out.CoreDown, out.Err = true, true, lo.Err
		endTyping()
		finish("", out.Err)
		return out, nil
	}

	model := workflow.WithActivityOptions(turnCtx, workflow.ActivityOptions{
		StartToCloseTimeout: modelTimeout(in.Turn), HeartbeatTimeout: 3 * heartbeatEvery,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3, InitialInterval: 2 * time.Second, BackoffCoefficient: 2,
			NonRetryableErrorTypes: []string{errAuth, errBadRequest},
		},
	})
	toolOpts := func(name string) workflow.Context {
		attempts := int32(1) // a call that may have an effect runs once
		if tools.Retryable(name) {
			attempts = 3
		}
		return workflow.WithActivityOptions(turnCtx, workflow.ActivityOptions{
			StartToCloseTimeout: in.Tool + 20*time.Second, HeartbeatTimeout: 3 * heartbeatEvery,
			RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: attempts, InitialInterval: time.Second},
		})
	}

	var state tools.State
	builtins := tools.New(tools.Options{}) // schedule_next and finish touch nothing outside the turn
	steps := agent.Steps{
		Complete: func(req llm.Request) (llm.Response, error) {
			var mo ModelOut
			err := workflow.ExecuteActivity(model, (*Acts).Model, ModelIn{
				Key: in.Key, Reason: in.Reason, Limited: in.Limited, Messages: req.Messages, MaxTokens: req.MaxTokens, Effort: req.Effort,
			}).Get(ctx, &mo)
			if err != nil {
				return llm.Response{}, modelError(err)
			}
			return mo.Resp, nil
		},
		Call: func(c llm.Block) (string, bool) {
			if tools.IsBuiltin(c.Name) {
				builtins.SetState(state)
				content, isErr := builtins.Call(context.Background(), c.Name, c.Input)
				state = builtins.State()
				return content, isErr
			}
			var to ToolOut
			err := workflow.ExecuteActivity(toolOpts(c.Name), (*Acts).Tool, ToolIn{
				Key: in.Key, Reason: in.Reason, Limited: in.Limited, Name: c.Name, Input: c.Input, State: state,
			}).Get(ctx, &to)
			if err != nil {
				if turnCtx.Err() != nil {
					return "the call was cancelled", true
				}
				// The worker died or the call ran out of time: it may have had its effect.
				return fmt.Sprintf("the call did not finish (%s). It may or may not have run: check before you do it again.", errText(err)), true
			}
			state = to.State
			return to.Content, to.IsError
		},
		Finished: func() bool { return state.Finished },
		Stopped:  turnCtx.Err,
		IsStop:   func(error) bool { return turnCtx.Err() != nil },
	}
	res := agent.RunLoop(steps, in.Caps, agent.Request{Model: in.Model}, agent.FirstMessage(in.Prompt, lo.Down, in.Caps), discard)
	endTyping()

	out = WakeOut{
		Text: res.Text, Summary: state.Summary, Next: state.Next,
		Steps: res.Steps, ToolCalls: res.ToolCalls,
		Input: res.Usage.Input, Output: res.Usage.Output, CacheRead: res.Usage.CacheRead, CacheWrite: res.Usage.CacheWrite,
		ContextTokens: res.ContextTokens, Model: res.Model, Seconds: workflow.Now(ctx).Sub(start).Seconds(),
		StepsExhausted: res.StepsExhausted, ToolLimit: res.ToolLimit, TokenLimit: res.TokenLimit,
		AuthFailed: res.AuthFailed, Failed: res.Failed,
	}
	if timedOut {
		out.TimedOut, out.Failed = true, true
		out.Err = fmt.Sprintf("the turn timed out after %s", in.Turn)
	} else if res.Err != nil {
		out.Err = res.Err.Error()
		if turnCtx.Err() != nil {
			out.Err, out.Failed = "the turn was cancelled", false
		}
	}
	finish(res.Text, out.Err)
	return out, nil
}

// modelTimeout bounds one model request: as long as the turn, and at least a
// few minutes (a thinking model can take one).
func modelTimeout(turn time.Duration) time.Duration {
	if turn < 4*time.Minute {
		return 4 * time.Minute
	}
	return turn
}

// modelError turns what a failed model activity says back into an error the
// loop reads: a refusal of the credentials stays one.
func modelError(err error) error {
	var ae *temporal.ApplicationError
	if errors.As(err, &ae) {
		switch ae.Type() {
		case errAuth:
			return &llm.APIError{Status: 401, Message: ae.Message()}
		case errBadRequest:
			return &llm.APIError{Status: 400, Message: ae.Message()}
		}
		return errors.New(ae.Message())
	}
	return errors.New(errText(err))
}
