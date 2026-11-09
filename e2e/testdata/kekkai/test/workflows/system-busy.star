workflow(name = "system-busy", start = "work")

# a script busy long enough for an instruction to arrive while it runs
state("work", script("scripts/sleep-ok.sh"), ok = "next", alt = "other")
state("next", done("next {note}"))
state("other", done("other {note}"))
