workflow(name = "timer-4s", start = "nap")

state("nap", wait(timer = "4s"), timer = "awake")
state("awake", done("awake"))
