# A wait keyed on {k}: with k unset it must catch no signal, not every one.
workflow(name = "keyed-wait", start = "wait_msg", inputs = {"k": "string?"})

state("wait_msg", wait(signal = "ping", key = "{k}", timeout = "3s"), signal = "got", timeout = "none")
state("got", done("got"))
state("none", done("none"))
