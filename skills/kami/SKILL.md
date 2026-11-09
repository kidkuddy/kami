---
name: kami
description: Drive the Kami agent OS from a Claude Code session — start workflow runs, see and resolve the decisions (contracts) waiting on the user, set AFK modes, publish signals, read a run's history, switch the active Kekkai domain. Use when the user says "kami", "start a kami run", "what's waiting on me", "resolve that contract", "afk this run", "kami status", or names a Kami workflow or Kekkai.
---

# kami — operate the daemon

Kami runs Starlark workflows (`~/.kami/kekkai/<domain>/workflows/*.star`)
whose steps are headless Claude/Codex workers or scripts. Decisions park as
**contracts** in a queue instead of in chat. Durable state lives in the
memento namespace `kami`; the event log in `~/.kami/events.db`.

Always pass `--json` when you parse output. Ids accept any unique prefix.

## Is it up?

```sh
kami status                 # daemon, run counts, open contracts, cron triggers
kami daemon start           # if it is not running (launchd normally keeps it up)
```

Exit code 6 means the daemon is unreachable.

## Domains (Kekkai)

```sh
kami kekkai list            # * marks the active one
kami kekkai switch <name>   # CLI default only; every domain keeps running
kami kekkai validate        # config, skills, workers, every workflow
kami kekkai reload          # after editing cron triggers
```

## Runs

```sh
kami workflow list                                   # in the active kekkai
kami run start <workflow> --input k=v [--input ...]  # values parse as JSON when they can
kami run start <workflow>:<entry> --input goal=x     # a named entry point
kami run start <workflow> --afk grace=10m            # AFK for this run
kami run list --status active                        # running, sleeping, waiting
kami run show <id>                                   # state, vars, contracts, aura, events
kami run cancel <id>                                 # cancels children too
kami run upgrade <id> [--state S] [--vars JSON]      # move a waiting run onto the edited workflow (vars merged in); an unchanged open question stays open
kami kekkai pause <k> [--until 09:00] [--except chat,converse]   # hold its runs (new steps, probes) until resume; cron fires meanwhile are skipped
kami kekkai resume <k>
```

A run's status: `running` (worker/script executing), `sleeping` (a worker
raised a contract and exited), `waiting` (decision, wait or child runs),
`done`, `failed`, `cancelled`. Terminal runs report `outcome` (the terminal
state's name) and `report`.

## Decisions waiting on the user

```sh
kami contract list                     # open ones; --status all for history
kami contract show <id>                # title, body, answers, fields, mode
kami contract resolve <id> <answer>    # answer = one of the listed answers
kami contract resolve <id> "free text" # an ask: any text
kami contract resolve <id> rated --field rating=good --field replay=true   # a form
```

Rules when acting for the user:

- **Never resolve a contract the user has not answered.** Show it (title,
  body verbatim, the answers) and ask. A contract marked `you only` (a
  `gate`, an `action`, or `lock=True`) exists precisely so nothing but the
  user decides it.
- Relay the body verbatim. It was written for someone who has not seen the work.
- `CONFLICT ... already resolved by resolver` means the AFK resolver got there
  first; report that instead of retrying.

## AFK

Each contract runs `attended` (only the user), `grace=<d>` (the user has d,
then the resolver answers) or `afk` (resolver at once). The workflow sets a
default; overrides win in this order: contract, run, workflow, kekkai default.

```sh
kami afk show <run>                          # effective mode per open contract
kami afk set afk --run <id>                  # this run's open + future contracts
kami afk set grace=30m --contract <id>       # one contract
kami afk set attended --run <id>             # back to the user
```

Locked contracts refuse `afk set` (exit 5). A resolver that fails hands the
contract back to `attended`; it is never dropped.

## Signals and history

```sh
kami signal pub <type> --key <correlation key> --data '{"k": "v"}'
kami events tail -f [--type contract.] [--run <id>]   # NDJSON with --json
```

`kami events tail -f --json` is the stream future adapters (jane, phoenix,
Slack) consume. `GET /events` with `Accept: text/event-stream` is the same log
as SSE: `id` is the event id, `event` its type, `data` the JSON, and
`Last-Event-ID` resumes after a reconnect.

## System instructions

```sh
kami system <run> "<why, for the log>"                   # wake: probe now
kami system <run> "<why>" --vars '{"k": "v"}'            # new vars, same state, latest workflow
kami system <run> "<why>" --state review                 # continue from another state
kami system <run> "<why>" --answer retry                 # answer its open contract
```

Every one is logged: a `system.instruction` event plus a `run.say` line in the
run's thread. Nothing else about the run changes. Workers cannot send one, and a
run holding a locked contract (a gate, an action, `lock=True`) refuses it, and
`--state` refuses a state the run could only reach through one: those stay the
owner's. A note to a run that is mid-step is logged only. `--vars` and `--state`
sent to a busy run (a process or children running) queue and apply when its step
ends, on its pinned workflow; the step's own vars lose to them and a queued state
replaces the step's next state. `--answer` still needs an open contract.

## Memory

```sh
kami memory save --about haithem [--scope public|private|dm:<user>|chan:<channel>] "hates tabs"
kami memory load [--about haithem] [--q tabs]
kami memory forget <id>
```

Shared by the owner (you, from the terminal: you hear everything; saves default to
`private`) and every worker. A run's conversation comes from its `kami_conv`,
`kami_peer` and `kami_channel` inputs, which workers can never set: a DM with a
coworker hears `public` and its own `dm:<user>`, a channel hears `public` and its
own `chan:<channel>`, and whatever they save stays in that conversation. When the
user says "remember X about Y", save it about Y; ask before making it `public`.

## Slack

`power` (POWER, github.com/kidkuddy/power: `make install` there) is Kami's Slack surface on niffty's
Slack app; niffty must be stopped while it serves.

```sh
power serve [--kekkai K] [--poll 5s]        # contracts → owner DMs; buttons / thread replies resolve them
      [--chat-kekkai work] [--chat-workflow chat] # every other owner DM → a chat run
power dm "text"                             # DM the owner (stdin when no args); prints {"channel","ts"}
power dm --thread <ts> "text"                # reply in a thread of the owner DM
power canvas [--key slug] "Title" < doc.md  # private canvas for the owner; prints its URL; same key = same canvas
```

Script steps use `dm` and `canvas` to reach the user.

A run lives in the Slack thread named by its var `thread_ts` (or the nearest
ancestor's, via `parent`). Its contracts are posted into that thread with their
buttons, and the thread gets one line per milestone: `run.say` text,
`↳ started <workflow>` for a child, `✅ <workflow>: <report>` /
`❌ <workflow> <status>: <report>` on finish. Thread-less runs: contracts as
top-level DMs, no milestones. A reply in a run thread answers the single open
contract there that takes text (an `ask` from a state or worker, `form`, or an option matching the
reply); none or several → chat.

Owner DMs that are not contract replies are chat, keyed by thread (`thread_ts`,
or the message's own ts at top level). Each is published as signal
`slack.message` (key = thread, `data: {text, ts, history}`) to the chat kekkai.
A chat run waiting on it gets it. If a chat run owns the thread but is busy,
the message is queued and retried each poll. With no run, a `chat` run starts
with inputs `thread_ts`, `channel`, `text`, `history` (`owner:` / `bot:` lines),
`thread_runs` (ids of the runs tied to the thread, comma-separated).
A chat workflow answers with `power dm --thread {thread_ts} "…"` and
waits on `slack.message` keyed by `{thread_ts}` for the next turn. A contract answered from
Slack shows `note: via slack (power)`.

### Audit

Every Slack message in and out (contracts, follow-ups, milestones, button taps with value
and answer, reactions, canvases, chat routing: `signalled` / `queued` /
`started` with run ids, errors) is one NDJSON line in
`$KAMI_HOME/adapters/slack-transcript.ndjson`, full text, no tokens.

```sh
power log [-f] [--run R] [--contract C] [--thread T] [--json]  # the transcript, filtered; -f follows
power threads [--since 24h] [--json]      # threads: root ts, first line, messages, last activity, runs, contracts
power thread <ts> [--json]                # one thread live from Slack, oldest first, with its contract/run ids
```

Then the Kami side: `kami run show <run>` (events, vars, contracts); each step
attempt's `~/.kami/runs/<run>/<state>.<visit>.<attempt>/` with `prompt.md`,
`stdout.log`, `stderr.log`; and Claude's session JSONL under
`$CLAUDE_CONFIG_DIR/projects/`, named by the `session_id` in `stdout.log`.

## When something is stuck

A run that cannot continue parks on an engine **blocker** contract
(answers `retry` / `stop`, or `continue` / `stop` for a loop budget). Its body
carries the error and the step's log directory; read
`<dir>/stderr.log` and `<dir>/prompt.md` there before retrying.
