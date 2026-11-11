# Kami 神

**A local agent OS that asks before it guesses.**

You write a workflow in Starlark. Kami runs it as a daemon on your machine: each step is a
headless Claude Code or Codex worker, or a plain script. When a worker reaches a decision that
belongs to you, it doesn't ask in chat and it doesn't guess. It posts a **contract**, saves a
small **Aura** (what it was doing, what's next), and exits. The run sleeps for free until you
answer. Then a fresh worker picks up from the Aura.

<p align="center"><img src="docs/hello.gif" width="760" alt="kami: start a run, read the draft the agent parked, keep it"></p>

| Name | What it is |
|---|---|
| Kamui | the control plane: loads `.star` workflows, validates them, moves runs between states. Code, not an LLM |
| Shikigami | a worker: `claude -p`, `codex exec`, or a fake script in tests |
| Contract | a decision in the queue: `ask`, `choice`, `form`, `blocker`, `gate`, `action` |
| AFK | per decision: `attended` (you), `grace=10m` (you, then a resolver), `afk` (resolver now) |
| Kekkai | a domain (`~/.kami/kekkai/<name>/`): its own env, prompt, skills, workflows, scripts |
| Aura | a worker's snapshot before it sleeps: goal, progress, next, vars |

Runs, contracts, Auras and the pinned workflow files live in the memento namespace `kami`, so a
crash or a reboot loses nothing: the daemon picks every run back up. The event log is
`~/.kami/events.db`.

## Build and run

```sh
make build                  # bin/kami
make install                # ~/.local/bin/kami + skills symlinked into ~/.claude/skills
make launchd-install        # keep `kami daemon` running (log: ~/.kami/daemon.log)
kami daemon start           # or run it by hand
kami status
```

Needs `memento` on PATH with an initialised store, and `claude` / `codex` for
real workers.

## Go toolchain

Kami is pinned to Go 1.27.1 via `go.mod` (`go 1.27.1` plus `toolchain go1.27.1`).
Building requires `GOTOOLCHAIN=auto` (or Go 1.27.1+ installed directly), since
older toolchains are refused with a clear message rather than a hard failure.
Note that `make` has no `GOTOOLCHAIN` handling: export `GOTOOLCHAIN=auto` before
running `make build`/`make test` if your installed Go is older than 1.27.1.

## A first workflow

```
~/.kami/kekkai/home/
  kekkai.yaml
  workflows/hello.star
```

```yaml
name: home
workers:
  writer: {provider: claude, model: claude-sonnet-5-5}
```

```python
workflow(name = "hello", inputs = {"topic": "string"}, start = "draft")

state("draft", agent("writer", "Write three lines about {topic} to draft.md, then report its absolute path as draft_path."),
      done = "review")
state("review", choice("Keep the draft?", body_file = "{draft_path}"), keep = "kept", redo = "draft")
state("kept", done("kept {draft_path}"))
```

```sh
kami kekkai switch home
kami workflow dry-run hello
kami run start hello --input topic=tea
kami contract list
kami contract resolve <id> keep
kami run show <run>
```

The DSL reference is in [`skills/kami-author/SKILL.md`](skills/kami-author/SKILL.md),
the operator's guide in [`skills/kami/SKILL.md`](skills/kami/SKILL.md), and the
worker protocol in [`skills/kami-worker/SKILL.md`](skills/kami-worker/SKILL.md).
Ported dry runs of real skills (acme flow, proxy-pr, Suno) are in
[`examples/kekkai/`](examples/kekkai/). The design and status are in
[`PLAN.md`](PLAN.md).

## Pause for the night

`kami kekkai pause` holds a whole kekkai. A run that reaches its next step stops there. Runs
already waiting stop probing, and their timeouts stop counting. Cron runs start held. Steps that
are already running finish, and open decisions can still be answered. `--except` keeps some
workflows going (your chats, say), and `--until` resumes the kekkai by itself.

<p align="center"><img src="docs/pause.gif" width="760" alt="kami kekkai pause home --until 09:00, then resume"></p>

```sh
kami kekkai pause work --until 09:00 --except chat,converse
kami kekkai resume work            # or wait for 09:00
```

## Slack: POWER

Kami's Slack surface is [POWER](https://github.com/kidkuddy/power), a separate repo and binary
(`power`). She DMs you every contract as buttons, threads each run, and routes your DMs to
chat runs. She talks to the daemon over its socket and holds no workflows of her own.

## Debugging a run

- `kami run show <run>`: its events, vars and contracts.
- `~/.kami/runs/<run>/<state>.<visit>.<attempt>/`: one directory per step
  attempt, with `prompt.md`, `stdout.log` and `stderr.log`.
- Claude's own session log: the JSONL under `$CLAUDE_CONFIG_DIR/projects/`
  named by the `session_id` in that attempt's `stdout.log`.
- `kami run upgrade <run> [--state S] [--vars JSON]`: after fixing a workflow, move a waiting
  run onto the new definition (and into a state) instead of starting over.

## Tests

```sh
make test     # unit + e2e: a real daemon per test, fake workers, temp memento stores
make race     # the same under the race detector
make smoke    # real Claude and Codex workers (uses tokens)
make bench    # memento write latency
```

Tests never touch `~/.kami` or your memento config: each one gets its own
`KAMI_HOME` and a `git init`-ed store through `MEMENTO_STORE`.
