# probe-wait with a 2s probe: ready about 4s in, so a pause under -race lands first (TestPause).
workflow(name = "probe-wait-slow", start = "poll")

state("poll", wait(probe = "scripts/probe.sh", every = "2s", timeout = "20s"),
      ready = "finished", timeout = "late")
state("finished", done("{ready_at}"))
state("late", fail("probe never became ready"))
