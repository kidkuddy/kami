# A 5s grace window that a network outage holds (TestOffline).
workflow(name = "afk-grace-5s", start = "approve",
         afk = afk("grace", grace = "5s", resolver = "resolvers/approve.sh"))

state("approve", choice("Approve the draft?"), approve = "approved", reject = "rejected")
state("approved", done("approved"))
state("rejected", done("rejected"))
