#!/bin/sh
# A stand-in for the Claude Code CLI. It records what it was given and emits
# stream-json the way `claude -p --output-format stream-json --verbose` does.
# Behaviour is steered by files in $FAKE_DIR:
#   mode (or mode.N)  ok | fail | nonext | garble | sleep | noresult | authfail | coredown | maxturns
#   tokens            "<input> <output>" per turn, default "100 50"
#   perm              permissionMode reported in init, default auto
#   next              next.json body, default {"after":"3m","why":"fake"}
d="$FAKE_DIR"
n=$(cat "$d/count" 2>/dev/null || echo 0)
n=$((n + 1))
echo "$n" > "$d/count"
printf '%s\n' "$@" > "$d/argv.$n"
cat > "$d/stdin.$n"
env > "$d/env.$n"
pwd > "$d/cwd.$n"

# The agent writes next.json at the state root; the CLI runs in <state>/work.
next="$(dirname "$PWD")/next.json"
sid=""
prev=""
for a in "$@"; do
  if [ "$prev" = "--resume" ]; then sid="$a"; fi
  prev="$a"
done
[ -n "$sid" ] || sid="sess-$n"

mode=ok
[ -f "$d/mode" ] && mode=$(cat "$d/mode")
[ -f "$d/mode.$n" ] && mode=$(cat "$d/mode.$n")
tokens="100 50"
[ -f "$d/tokens" ] && tokens=$(cat "$d/tokens")
set -- $tokens
tin=$1
tout=$2
perm=auto
[ -f "$d/perm" ] && perm=$(cat "$d/perm")
core=connected
[ "$mode" = coredown ] && core=failed

printf '{"type":"system","subtype":"init","session_id":"%s","model":"fake","permissionMode":"%s","mcp_servers":[{"name":"core","status":"%s"}]}\n' "$sid" "$perm" "$core"

case "$mode" in
  fail)
    echo "boom: simulated failure" >&2
    exit 1 ;;
  authfail)
    trap 'exit 143' TERM
    for i in 1 2 3 4 5 6; do
      printf '{"type":"system","subtype":"api_retry","attempt":%s,"max_retries":10,"retry_delay_ms":10,"error_status":401,"error":"authentication_failed","session_id":"%s"}\n' "$i" "$sid"
    done
    while true; do sleep 0.1; done ;;
  sleep)
    trap 'exit 143' TERM
    printf '{"type":"assistant","session_id":"%s","message":{"id":"m%s","usage":{"input_tokens":%s,"output_tokens":%s}}}\n' "$sid" "$n" "$tin" "$tout"
    echo started > "$d/sleeping.$n"
    while true; do sleep 0.1; done ;;
esac

printf '{"type":"assistant","session_id":"%s","message":{"id":"m%s","usage":{"input_tokens":%s,"output_tokens":%s}}}\n' "$sid" "$n" "$tin" "$tout"

case "$mode" in
  nonext) ;;
  garble) echo 'this is not json' > "$next" ;;
  *)
    if [ -f "$d/next" ]; then cp "$d/next" "$next"; else echo '{"after":"3m","why":"fake"}' > "$next"; fi ;;
esac

[ "$mode" = noresult ] && exit 0
if [ "$mode" = maxturns ]; then
  printf '{"type":"result","subtype":"error_max_turns","is_error":true,"session_id":"%s","num_turns":7,"result":"","total_cost_usd":0.01,"usage":{"input_tokens":%s,"output_tokens":%s}}\n' "$sid" "$tin" "$tout"
  exit 1
fi
printf '{"type":"result","subtype":"success","is_error":false,"session_id":"%s","num_turns":2,"result":"ok","total_cost_usd":0.01,"usage":{"input_tokens":%s,"output_tokens":%s}}\n' "$sid" "$tin" "$tout"
