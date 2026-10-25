package tools

// CoreTools is the core's tools the loop needs: the ones the instruction
// names, the read-only situation and memory tools, and the speech tools.
// Everything else the core offers is not given to the model, and the tools in
// CoreToolsDenied are refused by name even when they are also in the list.
// With --core-tools all neither list applies: the core's Role is the bound.
var CoreTools = []string{
	"get_triage_instructions", "get_tenant_situation", "get_tenant_health", "get_tenant_goals",
	"list_open_issues", "list_issues", "get_issue_detail", "get_issue_graph", "get_recent_events",
	"search_memory", "search_corpus", "get_memory", "get_memory_context", "get_memory_items", "get_memory_cell",
	"fetch_memory", "get_world_context", "get_service_analytics", "get_service_prediction",
	"inspect_service", "list_services", "list_applications", "list_pattern_proposals", "analyze_issue", "verify_hypothesis",
	"comment_on_issue", "upsert_memory_item", "note_observation",
	"say_in_workspace", "read_workspace_room", "read_workspace_file", "set_workspace_status", "set_spot_status",
}

// CoreToolsDenied are the core's tools the spot must never call: they close,
// forget or re-rate issues, change tenant settings, drive the core's own
// loops, or send test traffic. The instruction says so too, but a name the
// tool layer refuses is what holds under an injected instruction.
var CoreToolsDenied = []string{
	"resolve_issue", "forget_issue", "update_issue_severity", "link_issues",
	"set_tenant_goals", "run_rsi", "emit_event", "classify_event", "decide_event", "guardrail_event",
	"test_notify", "test_push", "approve_pattern", "link_pattern", "recognize_pattern",
	"expire_memory_item", "refute_memory_item", "supersede_memory_item", "consolidate_memory",
	"build_memory", "build_knowledge", "build_rca",
}

// EgoTools is the ego tools the spot may call: the ones that read sessions,
// threads, its own activity record, token usage, quality and findings. Corrections, labels, role
// bindings and running the findings loop are not here.
var EgoTools = []string{
	"list_metrics", "list_sessions", "list_threads", "summarize_session", "explain_session", "get_session_spans",
	"get_activity", "list_runs", "get_run_results", "get_usage", "get_quality", "explain_regression", "get_findings", "list_corrections",
}

// Server names. A tool is offered to the model as <server>__<tool>.
const (
	CoreServer = "core"
	EgoServer  = "ego"
	// Sep separates the server name from the tool name.
	Sep = "__"
)

// Built-in tool names.
const (
	ScheduleNext = "schedule_next"
	Finish       = "finish"
)

// upsertMemoryItem is the core tool that writes a memory item. SpotTagPrefix
// starts the tag the spot's items carry: spot:<name>.
const (
	upsertMemoryItem = "upsert_memory_item"
	SpotTagPrefix    = "spot:"
)
