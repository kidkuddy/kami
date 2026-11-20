# A script that errors {errors} times, then sets ticket_id; the engine retries it with backoff.
# `later` is reached only by a reopen.
workflow(name = "script-flaky", start = "call", inputs = {"errors": "int"}, entries = {"later": entry("later")})

state("call", script("scripts/flaky-script.sh {errors}"), ok = "finished")
state("finished", done("ok after retries"))
state("later", done("reopened"))
