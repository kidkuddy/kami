# A run tied to a Slack thread; its first step can claim another thread_ts (TestPauseAllow).
workflow(name = "threaded", inputs = {"thread_ts": "string", "claim": "string"}, start = "claim")

state("claim", script("scripts/claim.sh {claim}"), ok = "nap")
state("nap", wait(timer = "6s"), timer = "after")
state("after", done("ran"))
