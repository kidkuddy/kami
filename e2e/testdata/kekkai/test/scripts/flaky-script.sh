#!/bin/sh
# a script step: fails (exit 1, no JSON) the first $1 runs of this visit, then ok; the count
# lives in the run dir, since each attempt gets its own step dir
c="$(dirname "$KAMI_STEP_DIR")/flaky-count"
n=$(cat "$c" 2>/dev/null || echo 0)
n=$((n + 1))
echo "$n" > "$c"
if [ "$n" -le "$1" ]; then
  echo "service temporarily unavailable" >&2
  exit 1
fi
echo '{"outcome": "ok", "vars": {"ticket_id": "CAL-77"}}'
