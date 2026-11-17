#!/bin/sh
# sets vars.thread_ts to $1, as a worker's outcome could
echo "{\"outcome\": \"ok\", \"vars\": {\"thread_ts\": \"$1\"}}"
