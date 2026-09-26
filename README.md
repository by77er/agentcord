# agentcord

A multi-agent harness in Rust. Agents spawn other agents, message each other by id or a claimed
`@name`, and talk in durable, named pub/sub **topics**. Everything is written to a session directory
and a broker restart picks the session back up.

Each agent is a [pi](https://github.com/earendil-works/pi-mono) process in RPC mode (`pi --mode rpc`).
pi supplies the model providers, auth, coding tools, compaction and per-agent transcripts, and
agentcord adds the multi-agent layer:

```
 agentcord-tui ──HTTP/WS──┐
                          ▼
            ┌──────────── agentcord daemon (Rust) ────────────┐
 CLI ──────▶│ agents · names · topics · subscriptions · queues │◀── broker.sock ── pi extension tools
            │ events.jsonl + logs/*.jsonl (fsync'd, replayed)  │
            └───────┬───────────────────────┬─────────────────┘
          stdin/stdout JSONL (RPC)   stdin/stdout JSONL (RPC)
                ┌───▼───┐               ┌───▼───┐
                │ pi a-1│ ...           │ pi a-2│   each with its own pi session file
                └───────┘               └───────┘
```

Workspace: `crates/agentcord` (daemon + CLI), `crates/tui` (`agentcord-tui`), `crates/discord`
(`agentcord-discord`, a standalone Discord bridge), `crates/proto` (shared wire types).

## Quick start

```sh
cargo build --release
./target/release/agentcord-tui --start --model openai-codex/gpt-5.5 --name lead \
  "Spawn a researcher and a critic, create topic #plan, and ..."
```

`--start` launches a daemon for a new session (or the `--session` you pass). The daemon runs in its own
process group and keeps going after the TUI exits; stop it with `agentcord shutdown`. The optional
prompt spawns a root agent and opens its flow. Without `--start`, the TUI attaches to the newest
session's running daemon.

### TUI

The left column lists the agent tree (● running, ○ idle, ◌ dormant, ✗ killed, ✉ queued), topics and
DMs with unread counts, and an activity feed. The right pane is a live view:

- **Agent flow:** everything entering the agent's context, its thinking and text as they stream,
  and its tool calls and results.
- **Topic or DM:** the message log, updating live.

You are **@human**, a reserved name that no agent can claim. Agents can `send_message(to="@human")`.
DMs to you raise the `✉ N for @human` badge.

| Keys | |
|---|---|
| Tab / Shift-Tab | cycle focus: agents · topics & DMs · input |
| ↑↓ j k, Enter | select, open a live view |
| PgUp / PgDn, End | scroll, follow |
| s, t, x | `/spawn`, `/topic`, `/kill` selected |
| `text` | DM the viewed agent, or post to the viewed topic / DM |
| `@agent text`, `#topic text` | DM an agent, post to a topic |
| `/spawn [--name N] [--model M] [--thinking L] [--cwd D] [--tools a,b] <task>` | start an agent (a swarm) |
| `/topic <name> [desc]`, `/invite @a @b`, `/rename <name>` | manage topics |
| `/open @agent\|#topic`, `/dm @agent`, `/kill <agent>`, `?`, `q` | |

### HTTP / WebSocket API

The daemon serves a small API on `127.0.0.1` (random port; `--http ADDR` to pin it, `--no-http` to
disable). It writes `{url, token}` to `<session>/api.json` (mode 0600). Requests need
`Authorization: Bearer <token>` or `?token=`, and every request acts as @human.

```
GET  /api/state                       {session, agents[], logs[]}  (proto::Snapshot)
GET  /api/agents | /api/topics        listings
GET  /api/read?log=&before=&after=&limit=
POST /api/spawn         {prompt, name?, model?, thinking?, cwd?, tools?, system?}
POST /api/send          {to, text}
POST /api/post          {topic, text}
POST /api/topic         {name?, description?, invite?}
POST /api/topic/rename  {topic, name}
POST /api/topic/invite  {topic, agents}
POST /api/kill          {agent}
GET  /ws                               global activity feed         {"type":"feed","event":...}
GET  /ws?agent=<id|@name>[&backlog=N]  one agent's flow             {"type":"flow","live":bool,"item":...}
GET  /ws?log=<#topic|t-id|dm:..|@agent>[&backlog=N]  one log       {"type":"message","live":bool,"who":..,"msg":..}
```

Filtered streams send their backlog (`live: false`), then `{"type":"synced"}`, then live frames. The
agent flow backlog comes from `rpc.jsonl`. It is read under the same lock the live stream is
published from, so nothing is missed or repeated at the seam. Streaming `delta` items are live-only.

```sh
curl -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"to":"@lead","text":"status?"}' $URL/api/send
curl -N "${URL/http/ws}/ws?token=$TOKEN&agent=@lead"     # curl ≥ 7.86 speaks ws://
```

### CLI

```sh
./target/release/agentcord run --model openai-codex/gpt-5.5 --name lead \
  "Spawn a researcher and a critic, create topic #plan, and ..."
```

`run` is the no-TUI equivalent: it starts a daemon, spawns a root agent with your prompt, and streams
activity. Type a line to DM the
root agent. `@agent text` DMs another agent, `#topic text` posts, and `/agents`, `/topics`,
`/read <log> [before]`, `/kill <agent>` and `/quit` run commands. Pass `--exit-when-idle` for
scripted runs.

Or run the broker and use the client commands from other terminals:

```sh
agentcord serve --model openai-codex/gpt-5.5          # prints the session dir; export AGENTCORD_SESSION=...
agentcord spawn --name planner "Plan the refactor of src/auth"
agentcord send @planner "Also consider the session cookie code"
agentcord topic review --description "code review" --invite @planner
agentcord post '#review' "Kickoff: see PR 12"
agentcord topics                                        # topics, subscribers, your DMs
agentcord read '#review' -n 20 --before 40              # scroll back
agentcord read @planner                                 # your DM log with an agent
agentcord agents
agentcord watch -v                                      # live feed
agentcord kill @planner                                 # kills its descendants too
agentcord shutdown
agentcord serve --session .agentcord/sessions/<id>      # resume later
```

Client commands without `--session` / `$AGENTCORD_SESSION` use the newest session under
`./.agentcord/sessions`.

### Agent tooling flags (`serve`, `run`, and `agentcord-tui --start`)

- `--no-builtin-tools`: agents get none of pi's default tools (read, bash, edit, write, grep, find,
  ls), only the agentcord tools. Those tools are also excluded by name, so extensions that
  re-register them are removed too. This overrides `agent_spawn`'s `tools` allowlist.
- `--user-extensions`: load your globally installed pi extensions (e.g. iso) into agents. Off by
  default, and agentcord's own extension always loads.
- `--pi-arg=<arg>`: pass any other flag to every pi process.

These are saved in `session.json` and reused on resume. `--no-builtin-tools=false` turns the first
one back off.

## Discord bridge

`agentcord-discord` is a separate process that takes over one Discord channel and binds it to one
topic. It only talks to the daemon's HTTP/WS API:

- **Topic → channel.** Every post in the topic is mirrored through a channel webhook under the
  poster's name (`alice`, `bob`, …). Long posts are split at 2000 characters, and
  `@everyone`/user pings are suppressed.
- **Channel → topic.** Every message from a human (not bots or webhooks) is posted to the topic as
  **@human**. That wakes the topic's subscribers, and `@name` in the text wakes that agent.
  Attachments are forwarded as links.
- **No human echo.** @human posts are never mirrored out the webhook, whether they came from Discord,
  the TUI or the CLI. Only agents appear as webhook posters.
- **Takeover.** The bot sets the channel's topic line, posts a banner on first run, and creates (or
  reuses) a webhook named `agentcord`. The topic is created if it doesn't exist.
- **Durable.** `agentcord-discord-<channel>.json` stores the last mirrored seq and the last
  forwarded Discord message id. After a restart it mirrors what it missed and forwards channel
  messages written while it was down.

```sh
export DISCORD_TOKEN=...            # bot token; enable the Message Content intent in the developer portal
agentcord-discord --channel 123456789012345678 --topic '#plan' [--session DIR] [--replay 20] [--attribute]
```

The bot needs View Channel, Send Messages, Read Message History and Manage Webhooks (or pass
`--webhook-url`), plus Manage Channels to set the channel topic (optional; `--keep-channel-topic`
skips it). `--attribute` prefixes forwarded text with the Discord author's name, so agents can tell
people apart. `--avatar-template 'https://…?seed={name}'` gives each poster an avatar.

## Messaging model

| Thing | Addressed as | Delivery |
|---|---|---|
| Agent | `a-1b2c3d4e`, or `@name` once claimed (`claim_name`, or `name` at spawn) | — |
| Human | `@human` (reserved) | DMs are logged and shown in the TUI |
| DM | `send_message(to)`; log `dm:<a>+<b>`, readable as `@name` | wakes the recipient |
| Topic | `t-1b2c3d4e`, or `#name` once named (`topic_create`/`topic_name`) | subscribers: `wake` (default) or `digest`; invitees get a waking notice |
| Mention | `@name` anywhere in a post | wakes the mentioned agent |
| Report | automatic: a run's final answer goes to the spawning agent | wakes the parent |

- **Waking** means the messages are rendered into one `[agentcord] …` user turn and sent to the agent's
  pi process as a `steer` prompt. An idle agent starts a new run. A busy agent gets the messages
  between turns, after its current tool calls finish. A dormant agent (no process, e.g. after a
  restart) is started with `pi --continue` first.
- **Debounce.** Wake-ups for idle or dormant agents wait until no new message has arrived for 1s,
  capped at 5s after the first one. A burst becomes one turn, and a dormant agent's pi process boots
  during the wait. A spawn's task and messages to a running agent go out immediately. pi's steering
  mode is set to "all", so messages queued mid-run land together after the current turn. Tune with
  `serve --debounce-ms` / `--debounce-max-ms` (0 disables).
- **Digest** subscriptions don't wake anyone. Their posts queue up and ride along with the agent's next
  wake-up.
- **Topics** mirror agents. Each has a stable id and an optional unique name, and renaming keeps the
  history. Posting to a topic subscribes you, and posting to an unknown `#name` creates it. `topic_list`
  shows every topic, its subscribers, which ones you're subscribed to (and in which mode), and your DM
  conversations.
- **Logs** are ordered and every message has a `log#seq` address. `read_log(log, before, after, limit)`
  returns a window, the latest messages by default. `before=` scrolls back.
- **Waiting.** `wait_for_messages` ends a run silently, with no report. Agents use it after spawning
  children or when a wake-up needs no action.

Agent tools (from `extension/agentcord.ts`): `agent_spawn`, `agent_list`, `agent_kill`, `claim_name`,
`send_message`, `topic_create`, `topic_name`, `topic_invite`, `topic_list`, `subscribe`, `unsubscribe`, `post`, `read_log`,
`wait_for_messages`. All of pi's built-in tools (read, bash, edit, write, …) remain available.
`agent_spawn` accepts `tools` to restrict a child's built-in tools.

## Session directory

```
.agentcord/sessions/<stamp>/
  session.json            broker settings (model, pi args, cwd), reused on resume
  api.json                HTTP/WS url + token of the running daemon (0600)
  daemon.log              daemon output when started by agentcord-tui --start
  events.jsonl            every state change: agents, names, topics, subscriptions, routing, deliveries, processes
  logs/<t-id>.jsonl       topic messages
  logs/dm_<a>+<b>.jsonl   DM conversations (tasks and reports included)
  agents/<id>/
    meta.json, name.json  spawn parameters, current name
    system.md             the agent's appended system prompt
    pi/*.jsonl            pi's session: the full conversation incl. tool calls and results
    rpc.jsonl             raw RPC stream: commands sent + every pi event (minus streaming deltas)
    stderr.log
  pi-extension/agentcord.ts
  broker.sock
```

Messages and events are fsync'd before they take effect. On startup the broker replays
`events.jsonl` and `logs/`, then redelivers anything that was routed but never acknowledged by pi.
