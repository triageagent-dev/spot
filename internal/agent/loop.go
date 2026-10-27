package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/triageagent-dev/spot/internal/llm"
	"github.com/triageagent-dev/spot/internal/tools"
)

// Steps is what the loop needs from the world. The loop itself does no I/O and
// reads no clock, so the same code runs in this process (Agent.Run) and inside a
// durable workflow, where each of these is an activity.
type Steps struct {
	// Complete sends one model request.
	Complete func(req llm.Request) (llm.Response, error)
	// Call runs one tool call. A tool failure is a result, never an error.
	Call func(c llm.Block) (content string, isError bool)
	// Finished reports whether the model has called finish.
	Finished func() bool
	// Stopped says why the turn must end now (shutdown, time is up), or nil.
	Stopped func() error
	// IsStop reports whether an error of Complete is that stop.
	IsStop func(err error) bool
	// Take returns what a person added to the running turn since the last look;
	// FinishOrTake does the same and, when nothing is waiting, closes the window
	// in which a message can still be added. Both may be nil: nothing is added.
	Take         func() []string
	FinishOrTake func() []string
	// Text, when set, sees the text of each model answer (for debug logs).
	Text func(step int, text string)
}

// Caps are the per-turn limits.
type Caps struct {
	MaxSteps      int
	MaxToolCalls  int
	MaxTurnTokens int64
}

// Request is the fixed part of every model request of a turn.
type Request struct {
	System    []string
	Tools     []llm.ToolDef
	MaxTokens int
	Effort    string
	// Model names the model until the API names the one it used.
	Model string
}

// RunLoop is the agent loop: send the prompt, run the tools the model calls,
// send the results back, until the model is done or a cap is reached. first is
// the first user message. It always returns; failures are in the Result. It
// fills the counts, the usage and the end flags of the Result; the caller adds
// what depends on the world (the schedule, the summary, the duration).
func RunLoop(s Steps, c Caps, rq Request, first string, log *slog.Logger) (res Result) {
	res.Model = rq.Model
	msgs := []llm.Message{llm.UserText(first)}
	truncated, paused, told := false, false, false

	for res.Steps < c.MaxSteps {
		if err := stopped(s); err != nil {
			res.Err = err
			return res
		}
		elideOld(msgs)
		req := llm.Request{System: rq.System, Messages: msgs, Tools: rq.Tools, MaxTokens: rq.MaxTokens, Effort: rq.Effort}
		resp, err := s.Complete(req)
		if err != nil {
			res.Err = err
			if s.IsStop != nil && s.IsStop(err) {
				return res
			}
			res.Failed = true
			res.AuthFailed = llm.IsAuth(err)
			return res
		}
		res.Steps++
		res.Usage = res.Usage.Add(resp.Usage)
		res.ContextTokens = resp.Usage.Context()
		if resp.Model != "" {
			res.Model = resp.Model
		}
		res.Text = textOf(resp.Content)
		if s.Text != nil && res.Text != "" {
			s.Text(res.Steps, res.Text)
		}

		switch resp.StopReason {
		case llm.StopToolUse:
			calls := toolUses(resp.Content)
			if len(calls) == 0 {
				if endOrInject(s, &msgs, resp) {
					return res
				}
				continue
			}
			if told && !onlyBuiltins(calls) {
				// The model was told the cap was reached and still asks for tools.
				res.ToolLimit = true
				res.TokenLimit = res.ToolCalls < c.MaxToolCalls && overTokens(c, &res)
				log.Warn("the turn stopped at its cap", "toolCalls", res.ToolCalls, "billable", res.Usage.Billable(), "tokenCap", res.TokenLimit)
				return res
			}
			msgs = append(msgs, llm.Message{Role: llm.RoleAssistant, Content: resp.Content})
			results, refused := runTools(s, c, calls, &res)
			if refused && !told {
				told = true
				log.Warn("turn cap reached; the model is told to wrap up", "toolCalls", res.ToolCalls, "cap", c.MaxToolCalls, "billable", res.Usage.Billable(), "tokenCap", c.MaxTurnTokens)
			}
			if s.Finished != nil && s.Finished() {
				return res
			}
			if len(results) == 0 {
				return res
			}
			if s.Take != nil {
				for _, extra := range s.Take() {
					results = append(results, llm.Text(extra))
				}
			}
			msgs = append(msgs, llm.Message{Role: llm.RoleUser, Content: results})

		case llm.StopMaxTokens:
			if truncated {
				res.Failed, res.Err = true, errors.New("the model's answer was cut at the output cap twice")
				return res
			}
			truncated = true
			// A tool call cut in the middle has no valid input: drop it. What is
			// left (thinking, text) is kept and the model is asked to go on.
			keep := withoutToolUses(resp.Content)
			if len(keep) > 0 {
				msgs = append(msgs, llm.Message{Role: llm.RoleAssistant, Content: keep})
			}
			msgs = append(msgs, llm.UserText(continueText))

		case llm.StopRefusal:
			res.Failed = true
			res.Err = errors.New("the model refused the request" + categoryText(resp.RefusalCategory))
			return res

		case llm.StopPauseTurn:
			if paused {
				res.Failed, res.Err = true, errors.New("the model paused the turn twice")
				return res
			}
			paused = true
			msgs = append(msgs, llm.Message{Role: llm.RoleAssistant, Content: resp.Content})

		default: // end_turn, and any stop with nothing left to do
			if endOrInject(s, &msgs, resp) {
				return res
			}
		}
	}
	res.StepsExhausted = true
	log.Warn("the turn stopped at the step cap", "steps", res.Steps)
	return res
}

func stopped(s Steps) error {
	if s.Stopped == nil {
		return nil
	}
	return s.Stopped()
}

// endOrInject ends the turn, unless a person's message arrived while the model
// was answering: then it is added to the conversation and the turn goes on.
// It reports whether the turn ends.
func endOrInject(s Steps, msgs *[]llm.Message, resp llm.Response) bool {
	if s.FinishOrTake == nil {
		return true
	}
	extra := s.FinishOrTake()
	if len(extra) == 0 {
		return true
	}
	if len(resp.Content) > 0 {
		*msgs = append(*msgs, llm.Message{Role: llm.RoleAssistant, Content: resp.Content})
	}
	blocks := make([]llm.Block, 0, len(extra))
	for _, e := range extra {
		blocks = append(blocks, llm.Text(e))
	}
	*msgs = append(*msgs, llm.Message{Role: llm.RoleUser, Content: blocks})
	return false
}

// runTools runs the calls of one assistant message, in order, and returns the
// tool_result blocks for all of them.
// Past the cap a call is answered with an error and not run, except the
// spot's own schedule_next and finish; refused reports whether any was.
func runTools(s Steps, c Caps, calls []llm.Block, res *Result) (out []llm.Block, refused bool) {
	out = make([]llm.Block, len(calls))
	held := false // a call of this batch was held back by a gate and did not run
	var finishes []int
	for i, call := range calls {
		if tools.IsFinish(call.Name) {
			finishes = append(finishes, i) // last, once the others have shown whether they ran
			continue
		}
		if text := limitText(c, res); text != "" && !tools.IsBuiltin(call.Name) {
			refused = true
			out[i] = llm.ToolResult(call.ID, text, true)
			continue
		}
		content, isErr := callTool(s, call, res)
		held = held || (isErr && tools.Held(content))
		out[i] = llm.ToolResult(call.ID, content, isErr)
	}
	for _, i := range finishes {
		call := calls[i]
		if held {
			// The turn must not end on a post or a read that did not run: the
			// model would report it as done. It sees why and goes on.
			out[i] = llm.ToolResult(call.ID, finishHeldText, true)
			continue
		}
		content, isErr := callTool(s, call, res)
		out[i] = llm.ToolResult(call.ID, content, isErr)
	}
	return out, refused
}

// finishHeldText answers a finish that came in the same step as a call a gate
// held back.
const finishHeldText = "not finished: a call in this step did not run (see its result). Do what it says and call it again, " +
	"or, if you will not do it, say so plainly in your summary; do not report as done what did not run. Then call finish."

// callTool runs one tool call of the model and counts it.
func callTool(s Steps, c llm.Block, res *Result) (content string, isErr bool) {
	if !json.Valid(c.Input) {
		content, isErr = "the tool input was not valid JSON", true
	} else {
		content, isErr = s.Call(c)
	}
	res.ToolCalls++
	return content, isErr
}

// overTokens reports whether the turn has spent its token cap.
func overTokens(c Caps, res *Result) bool {
	return c.MaxTurnTokens > 0 && res.Usage.Billable() >= c.MaxTurnTokens
}

// limitText is the refusal for a tool call when a cap is spent, or empty.
func limitText(c Caps, res *Result) string {
	if res.ToolCalls >= c.MaxToolCalls {
		return fmt.Sprintf(toolLimitText, c.MaxToolCalls)
	}
	if overTokens(c, res) {
		return fmt.Sprintf(tokenLimitText, c.MaxTurnTokens)
	}
	return ""
}
