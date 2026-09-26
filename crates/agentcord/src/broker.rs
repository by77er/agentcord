//! The broker owns all session state: agents, topics, names, logs, subscriptions and delivery queues.
//!
//! Every state change is appended to `events.jsonl` (and messages to `logs/*.jsonl`) before it
//! takes effect, so a restarted broker rebuilds the same session by replaying those files.
//! Agents are `pi --mode rpc` subprocesses. They are started lazily, when a message that should
//! wake them arrives, and resumed from their own pi session file with `--continue`.

use crate::flow;
use crate::store::{self, Jsonl};
use agentcord_proto::*;
use anyhow::{Context, Result, anyhow, bail};
use regex::Regex;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{Notify, broadcast, mpsc, oneshot};

const EXTENSION_SOURCE: &str = include_str!("../extension/agentcord.ts");
/// Tool names the extension registers; kept in sync with extension/agentcord.ts.
const EXTENSION_TOOLS: &[&str] = &[
    "agent_spawn",
    "agent_list",
    "agent_kill",
    "claim_name",
    "send_message",
    "post",
    "read_log",
    "topic_create",
    "topic_name",
    "topic_invite",
    "topic_list",
    "subscribe",
    "unsubscribe",
    "wait_for_messages",
];
/// pi's default tools. Excluded by name (not just `--no-builtin-tools`) so extensions that
/// re-register them as overrides are removed too.
const PI_BUILTIN_TOOLS: &str = "read,bash,edit,write,grep,find,ls";
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(180);

static MENTION: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?:^|[^\w@])@([A-Za-z0-9][A-Za-z0-9_-]*)").unwrap());
static NAME: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[a-z][a-z0-9_-]{0,31}$").unwrap());
static ID_LIKE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[at]-[0-9a-f]+$").unwrap());

#[derive(Clone, Debug)]
pub struct Config {
    pub session: PathBuf,
    pub pi_bin: String,
    pub pi_args: Vec<String>,
    pub model: Option<String>,
    pub thinking: Option<String>,
    pub cwd: PathBuf,
    /// Run agents without pi's default tools (read, bash, edit, write, grep, find, ls).
    pub no_builtin_tools: bool,
    /// Load the user's globally installed pi extensions into agents (off by default).
    pub user_extensions: bool,
    /// Wake debounce for idle/dormant agents: deliver once no new waking message has arrived for
    /// `debounce`, but hold the first one at most `debounce_max`. Zero disables it.
    pub debounce: Duration,
    pub debounce_max: Duration,
}

pub struct Broker {
    pub cfg: Config,
    st: Mutex<State>,
    pub feed: broadcast::Sender<Feed>,
    /// Per-agent flow items (agent id, item), for `/ws?agent=`.
    pub flow: broadcast::Sender<(String, FlowItem)>,
    /// Each agent's `rpc.jsonl`. Its lock also orders flow broadcasts against backlog reads.
    rpc_files: Mutex<HashMap<String, Arc<Mutex<Jsonl>>>>,
    pub shutdown: Notify,
    ext_path: PathBuf,
    socket: PathBuf,
}

struct State {
    events: Jsonl,
    agents: BTreeMap<String, Agent>,
    /// name -> agent id
    names: HashMap<String, String>,
    topics: BTreeMap<String, Topic>,
    /// name -> topic id
    topic_names: HashMap<String, String>,
    /// Topic ids and DM logs (`dm:<a>+<b>`) -> messages.
    logs: BTreeMap<String, Log>,
    next_req: u64,
    next_gen: u64,
    shutting_down: bool,
    /// Agents woken as a side effect of a request (e.g. invite notices), drained by `handle`.
    kicks: Vec<String>,
}

struct Agent {
    meta: AgentMeta,
    name: Option<String>,
    subs: BTreeMap<String, SubMode>,
    pending: Vec<Pending>,
    killed: bool,
    proc: Option<Proc>,
    running: bool,
    /// Deliveries written to pi but not yet acknowledged.
    inflight: usize,
    /// Final answers (assistant messages that ended with `stop`) of the current run. A run can
    /// answer several times when messages are steered in after it first finished.
    final_text: Vec<String>,
    /// When the oldest / newest undelivered waking message arrived (debounce window).
    first_wake: Option<Instant>,
    last_wake: Option<Instant>,
    /// Skip the debounce for the next delivery (a freshly spawned agent's task).
    urgent: bool,
    /// A delivery task is already waiting out the debounce window.
    debouncing: bool,
}

#[derive(Clone, Debug)]
struct Pending {
    log: String,
    seq: u64,
    wake: bool,
}

struct Proc {
    generation: u64,
    stdin: mpsc::UnboundedSender<Value>,
    waiters: HashMap<String, oneshot::Sender<Value>>,
    kill: Option<oneshot::Sender<()>>,
}

struct Topic {
    meta: TopicMeta,
    name: Option<String>,
}

struct Log {
    file: Jsonl,
    msgs: Vec<Message>,
}

impl Agent {
    fn status(&self) -> Status {
        match (self.killed, &self.proc, self.running) {
            (true, _, _) => Status::Killed,
            (_, None, _) => Status::Dormant,
            (_, Some(_), true) => Status::Running,
            (_, Some(_), false) => Status::Idle,
        }
    }
}

/// Agent id, backlog, and the live flow channel (filter by agent id).
pub type FlowSubscription = (
    String,
    Vec<FlowItem>,
    broadcast::Receiver<(String, FlowItem)>,
);
/// Log key, backlog of (sender label, message), and the live feed (filter by log).
pub type LogSubscription = (String, Vec<(String, Message)>, broadcast::Receiver<Feed>);

/// Recipients of a message: agent id -> whether it wakes them.
type Recipients = BTreeMap<String, bool>;

impl Broker {
    pub fn open(cfg: Config) -> Result<Arc<Self>> {
        let session = &cfg.session;
        fs::create_dir_all(session.join("agents"))?;
        fs::create_dir_all(session.join("logs"))?;
        let ext_path = session.join("pi-extension").join("agentcord.ts");
        fs::create_dir_all(ext_path.parent().unwrap())?;
        fs::write(&ext_path, EXTENSION_SOURCE)?;

        let events_path = session.join("events.jsonl");
        let mut st = State {
            events: Jsonl::open(&events_path, true)?,
            agents: BTreeMap::new(),
            names: HashMap::new(),
            topics: BTreeMap::new(),
            topic_names: HashMap::new(),
            logs: BTreeMap::new(),
            next_req: 0,
            next_gen: 0,
            shutting_down: false,
            kicks: Vec::new(),
        };
        for ev in store::read_all::<Event>(&events_path)? {
            st.replay(ev);
        }
        for entry in fs::read_dir(session.join("logs"))? {
            let path = entry?.path();
            if path.extension().and_then(|e| e.to_str()) != Some("jsonl") {
                continue;
            }
            let msgs: Vec<Message> = store::read_all(&path)?;
            let Some(name) = msgs.first().map(|m| m.log.clone()) else {
                continue;
            };
            st.logs.insert(
                name,
                Log {
                    file: Jsonl::open(&path, true)?,
                    msgs,
                },
            );
        }

        let (feed, _) = broadcast::channel(4096);
        let (flow, _) = broadcast::channel(16384);
        Ok(Arc::new(Self {
            socket: session.join("broker.sock"),
            cfg,
            st: Mutex::new(st),
            feed,
            flow,
            rpc_files: Mutex::new(HashMap::new()),
            shutdown: Notify::new(),
            ext_path,
        }))
    }

    fn rpc_file(&self, id: &str) -> Result<Arc<Mutex<Jsonl>>> {
        let mut files = self.rpc_files.lock().unwrap();
        if let Some(f) = files.get(id) {
            return Ok(f.clone());
        }
        let f = Arc::new(Mutex::new(Jsonl::open(
            &self.agent_dir(id).join("rpc.jsonl"),
            false,
        )?));
        files.insert(id.to_string(), f.clone());
        Ok(f)
    }

    /// Subscribe to an agent's flow: its last `limit` recorded items, then live items.
    /// The rpc file lock is held across the read so nothing is missed or repeated.
    pub fn subscribe_flow(&self, spec: &str, limit: usize) -> Result<FlowSubscription> {
        let id = {
            let st = self.st.lock().unwrap();
            let id = st.resolve_agent(spec)?;
            st.require_agent(&id)?;
            id
        };
        let file = self.rpc_file(&id)?;
        let _guard = file.lock().unwrap();
        let rx = self.flow.subscribe();
        let path = self.agent_dir(&id).join("rpc.jsonl");
        let items: Vec<FlowItem> = store::read_all::<Value>(&path)?
            .iter()
            .filter_map(flow::from_pi_event)
            .collect();
        let start = items.len().saturating_sub(limit);
        Ok((id, items[start..].to_vec(), rx))
    }

    /// Subscribe to a topic or DM log: its last `limit` messages (with sender labels), then the
    /// live feed. The state lock is held so no message falls between backlog and subscription.
    pub fn subscribe_log(&self, spec: &str, limit: usize) -> Result<LogSubscription> {
        let st = self.st.lock().unwrap();
        let log = st.resolve_log(HUMAN, spec)?;
        let rx = self.feed.subscribe();
        let msgs = st.logs.get(&log).map(|l| l.msgs.as_slice()).unwrap_or(&[]);
        let backlog = msgs[msgs.len().saturating_sub(limit)..]
            .iter()
            .map(|m| (st.label(&m.from), m.clone()))
            .collect();
        Ok((log, backlog, rx))
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket
    }

    /// After a restart, deliver anything that was routed but never accepted.
    pub fn resume(self: &Arc<Self>) -> Vec<String> {
        let ids: Vec<String> = {
            let st = self.st.lock().unwrap();
            st.agents
                .values()
                .filter(|a| !a.killed && a.pending.iter().any(|p| p.wake))
                .map(|a| a.meta.id.clone())
                .collect()
        };
        for id in &ids {
            self.kick(id.clone());
        }
        ids
    }

    /// True when no agent is running, delivering, or has a waking message queued.
    pub fn is_quiescent(&self) -> bool {
        let st = self.st.lock().unwrap();
        st.agents.values().filter(|a| !a.killed).all(|a| {
            !a.running && a.inflight == 0 && !a.debouncing && !a.pending.iter().any(|p| p.wake)
        })
    }

    fn emit(&self, ev: Feed) {
        let _ = self.feed.send(ev);
    }

    fn agent_dir(&self, id: &str) -> PathBuf {
        self.cfg.session.join("agents").join(id)
    }

    // ---------------------------------------------------------------- requests

    pub fn handle(self: &Arc<Self>, req: Request) -> Response {
        let mut kicks = Vec::new();
        let result = {
            let mut st = self.st.lock().unwrap();
            let result = self.dispatch(&mut st, req, &mut kicks);
            kicks.append(&mut st.kicks);
            result
        };
        for id in kicks {
            self.kick(id);
        }
        result.unwrap_or_else(Response::err)
    }

    fn dispatch(&self, st: &mut State, req: Request, kicks: &mut Vec<String>) -> Result<Response> {
        match req {
            Request::Spawn {
                from,
                prompt,
                name,
                model,
                thinking,
                cwd,
                tools,
                system,
                report,
            } => {
                let spec = SpawnSpec {
                    prompt,
                    name,
                    model,
                    thinking,
                    cwd,
                    tools,
                    system,
                    report,
                };
                let (id, rcpt) = self.spawn_agent(st, &from, spec)?;
                collect_kicks(&rcpt, kicks);
                let who = st.label(&id);
                Ok(Response::ok(
                    format!(
                        "Spawned {who}. It is working on the task now; its final response will be delivered to {} as a report.",
                        st.label(&from)
                    ),
                    json!({ "id": id }),
                ))
            }
            Request::Send { from, to, text } => {
                let log = st.resolve_log(&from, &format!("@{}", to.trim_start_matches('@')))?;
                let (msg, rcpt) = self.post(st, &from, &log, text, MsgKind::Message)?;
                collect_kicks(&rcpt, kicks);
                Ok(Response::ok(
                    sent_text(st, &msg, &rcpt),
                    json!({ "message": msg }),
                ))
            }
            Request::Post { from, topic, text } => {
                let id = match st.resolve_topic(&topic) {
                    Ok(id) => id,
                    // Posting to an unknown #name creates that topic.
                    Err(_) if topic.trim().starts_with('#') => {
                        let name = topic.trim().trim_start_matches('#').to_string();
                        self.create_topic(st, &from, Some(name), None, &[])?
                    }
                    Err(e) => return Err(e),
                };
                if from != HUMAN && !st.require_agent(&from)?.subs.contains_key(&id) {
                    self.subscribe(st, &from, &id, SubMode::Wake, &from)?;
                }
                let (msg, rcpt) = self.post(st, &from, &id, text, MsgKind::Message)?;
                collect_kicks(&rcpt, kicks);
                Ok(Response::ok(
                    sent_text(st, &msg, &rcpt),
                    json!({ "message": msg }),
                ))
            }
            Request::TopicCreate {
                from,
                name,
                description,
                invite,
            } => {
                let invite = invite
                    .iter()
                    .map(|a| st.resolve_agent(a))
                    .collect::<Result<Vec<_>>>()?;
                let id = self.create_topic(st, &from, name, description, &invite)?;
                Ok(Response::ok(
                    format!(
                        "Created topic {}. Subscribers are woken by each post; post(topic, text) to start.",
                        st.topic_label(&id)
                    ),
                    json!({ "id": id }),
                ))
            }
            Request::TopicName { from, topic, name } => {
                let id = st.resolve_topic(&topic)?;
                self.name_topic(st, &from, &id, &name, true)?;
                Ok(Response::ok(
                    format!("Topic {id} is now #{name}."),
                    json!({ "id": id, "name": name }),
                ))
            }
            Request::Topics { from } => Ok(st.list_topics(&from)),
            Request::Invite {
                from,
                topic,
                agents,
            } => {
                if from != HUMAN {
                    st.require_agent(&from)?;
                }
                let id = st.resolve_topic(&topic)?;
                let agents = agents
                    .iter()
                    .map(|a| st.resolve_agent(a))
                    .collect::<Result<Vec<_>>>()?;
                for agent in agents.iter().filter(|a| *a != HUMAN) {
                    self.subscribe(st, agent, &id, SubMode::Wake, &from)?;
                }
                let labels: Vec<String> = agents.iter().map(|a| st.label(a)).collect();
                Ok(Response::ok(
                    format!(
                        "Subscribed {} to {}.",
                        labels.join(", "),
                        st.topic_label(&id)
                    ),
                    json!({ "topic": id, "agents": agents }),
                ))
            }
            Request::ClaimName { from, name } => {
                st.require_agent(&from)?;
                self.claim_name(st, &from, &name)?;
                Ok(Response::ok(
                    format!("You are now @{name}."),
                    json!({ "name": name }),
                ))
            }
            Request::Subscribe { from, topic, mode } => {
                st.require_agent(&from)?;
                let id = st.resolve_topic(&topic)?;
                let mode = mode.unwrap_or(SubMode::Wake);
                self.subscribe(st, &from, &id, mode, &from)?;
                Ok(Response::ok(
                    format!(
                        "Subscribed to {} ({}).",
                        st.topic_label(&id),
                        mode_str(mode)
                    ),
                    json!({ "topic": id, "mode": mode }),
                ))
            }
            Request::Unsubscribe { from, topic } => {
                st.require_agent(&from)?;
                let id = st.resolve_topic(&topic)?;
                if st.agents[&from].subs.contains_key(&id) {
                    st.events.append(&Event::Unsubscribed {
                        ts: store::now(),
                        agent: from.clone(),
                        topic: id.clone(),
                    })?;
                    st.agents.get_mut(&from).unwrap().subs.remove(&id);
                    self.emit(Feed::Unsubscribed {
                        who: st.label(&from),
                        topic: st.topic_label(&id),
                    });
                }
                Ok(Response::ok(
                    format!("Unsubscribed from {}.", st.topic_label(&id)),
                    json!({ "topic": id }),
                ))
            }
            Request::Read {
                from,
                log,
                before,
                after,
                limit,
            } => {
                let log = st.resolve_log(&from, &log)?;
                Ok(st.read_window(&log, before, after, limit))
            }
            Request::Agents { from } => Ok(st.list_agents(&from)),
            Request::State => Ok(Response::ok(
                "",
                serde_json::to_value(st.snapshot(&self.cfg.session))?,
            )),
            Request::Kill { from, agent } => {
                let target = st.resolve_agent(&agent)?;
                let killed = self.kill(st, &from, &target)?;
                let labels: Vec<String> = killed.iter().map(|id| st.label(id)).collect();
                Ok(Response::ok(
                    format!("Killed {}.", labels.join(", ")),
                    json!({ "killed": killed }),
                ))
            }
            Request::Watch => bail!("watch is only available on a streaming connection"),
            Request::Shutdown => {
                self.stop_all(st);
                self.shutdown.notify_waiters();
                Ok(Response::ok("Shutting down.", Value::Null))
            }
        }
    }

    // ---------------------------------------------------------------- agents

    fn spawn_agent(
        &self,
        st: &mut State,
        from: &str,
        spec: SpawnSpec,
    ) -> Result<(String, Recipients)> {
        let parent = if from == HUMAN {
            None
        } else {
            Some(st.require_agent(from)?.meta.clone())
        };
        if let Some(name) = &spec.name {
            st.check_name(name, None)?;
        }
        let base = parent
            .as_ref()
            .map(|p| PathBuf::from(&p.cwd))
            .unwrap_or(self.cfg.cwd.clone());
        let cwd = match &spec.cwd {
            Some(c) => base.join(c),
            None => base,
        };
        let cwd = cwd
            .canonicalize()
            .with_context(|| format!("cwd {}", cwd.display()))?;
        if !cwd.is_dir() {
            bail!("cwd {} is not a directory", cwd.display());
        }

        let id = new_agent_id(st);
        let meta = AgentMeta {
            id: id.clone(),
            parent: from.to_string(),
            created: store::now(),
            cwd: cwd.display().to_string(),
            model: spec
                .model
                .or_else(|| parent.as_ref().and_then(|p| p.model.clone())),
            thinking: spec
                .thinking
                .or_else(|| parent.as_ref().and_then(|p| p.thinking.clone())),
            tools: spec.tools,
            system: spec.system,
            report: spec.report.unwrap_or(true),
        };
        st.events.append(&Event::AgentCreated {
            ts: store::now(),
            meta: meta.clone(),
        })?;
        st.agents.insert(id.clone(), Agent::new(meta.clone()));

        let dir = self.agent_dir(&id);
        fs::create_dir_all(dir.join("pi"))?;
        store::write_json(&dir.join("meta.json"), &meta)?;
        let mut system = system_prompt(st, &meta, spec.name.as_deref());
        if self.cfg.no_builtin_tools {
            system.push_str(
                "- This session runs without pi's built-in tools: you have no file or shell access. Work through \
                 the agentcord tools and your own reasoning.\n",
            );
        }
        fs::write(dir.join("system.md"), system)?;

        if let Some(name) = &spec.name {
            self.claim_name(st, &id, name)?;
        }
        self.emit(Feed::AgentCreated {
            id: id.clone(),
            who: st.label(&id),
            parent: st.label(from),
        });

        let log = dm_log(from, &id);
        let (_, rcpt) = self.post(st, from, &log, spec.prompt, MsgKind::Task)?;
        st.agents.get_mut(&id).unwrap().urgent = true;
        Ok((id, rcpt))
    }

    fn create_topic(
        &self,
        st: &mut State,
        from: &str,
        name: Option<String>,
        description: Option<String>,
        invite: &[String],
    ) -> Result<String> {
        if from != HUMAN {
            st.require_agent(from)?;
        }
        if let Some(name) = &name {
            st.check_topic_name(name, None)?;
        }
        let id = loop {
            let id = format!("t-{}", &uuid::Uuid::new_v4().simple().to_string()[..8]);
            if !st.topics.contains_key(&id) {
                break id;
            }
        };
        let meta = TopicMeta {
            id: id.clone(),
            created: store::now(),
            created_by: from.into(),
            description,
        };
        st.events.append(&Event::TopicCreated {
            ts: store::now(),
            meta: meta.clone(),
        })?;
        st.topics.insert(id.clone(), Topic { meta, name: None });
        if let Some(name) = name {
            self.name_topic(st, from, &id, &name, false)?;
        }
        self.emit(Feed::TopicCreated {
            id: id.clone(),
            topic: st.topic_label(&id),
            by: st.label(from),
        });
        if from != HUMAN {
            self.subscribe(st, from, &id, SubMode::Wake, from)?;
        }
        for agent in invite {
            if agent != from && agent != HUMAN {
                self.subscribe(st, agent, &id, SubMode::Wake, from)?;
            }
        }
        Ok(id)
    }

    /// Name or rename a topic. Its creator (or the user) may rename it; anyone may name an unnamed one.
    fn name_topic(
        &self,
        st: &mut State,
        from: &str,
        id: &str,
        name: &str,
        announce: bool,
    ) -> Result<()> {
        st.check_topic_name(name, Some(id))?;
        let topic = &st.topics[id];
        if topic.name.is_some() && from != HUMAN && topic.meta.created_by != from {
            bail!(
                "only the topic's creator ({}) can rename {}",
                st.label(&topic.meta.created_by),
                st.topic_label(id)
            );
        }
        if topic.name.as_deref() == Some(name) {
            return Ok(());
        }
        st.events.append(&Event::TopicNamed {
            ts: store::now(),
            topic: id.into(),
            name: name.into(),
            by: from.into(),
        })?;
        st.rename_topic(id, name);
        if announce {
            self.emit(Feed::TopicNamed {
                id: id.into(),
                name: name.into(),
                by: st.label(from),
            });
        }
        Ok(())
    }

    fn subscribe(
        &self,
        st: &mut State,
        agent: &str,
        topic: &str,
        mode: SubMode,
        by: &str,
    ) -> Result<()> {
        let a = st.require_agent(agent)?;
        if a.killed || a.subs.get(topic) == Some(&mode) {
            return Ok(());
        }
        st.events.append(&Event::Subscribed {
            ts: store::now(),
            agent: agent.into(),
            topic: topic.into(),
            mode,
            by: by.into(),
        })?;
        st.agents
            .get_mut(agent)
            .unwrap()
            .subs
            .insert(topic.into(), mode);
        self.emit(Feed::Subscribed {
            who: st.label(agent),
            topic: st.topic_label(topic),
            mode,
        });
        if by != agent {
            // Tell the invitee, so it knows the topic exists and wakes to engage with it.
            let about = st.topics[topic]
                .meta
                .description
                .as_ref()
                .map(|d| format!(" ({d})"))
                .unwrap_or_default();
            let text = format!(
                "{} subscribed you to topic {}{about}. Posts there will wake you; read_log(\"{topic}\") shows its \
                 history and unsubscribe(\"{topic}\") leaves it.",
                st.label(by),
                st.topic_label(topic),
            );
            let (_, rcpt) = self.post(st, by, &dm_log(by, agent), text, MsgKind::System)?;
            collect_kicks(&rcpt, &mut st.kicks);
        }
        Ok(())
    }

    fn claim_name(&self, st: &mut State, id: &str, name: &str) -> Result<()> {
        st.check_name(name, Some(id))?;
        let old = st.agents.get(id).and_then(|a| a.name.clone());
        if old.as_deref() == Some(name) {
            return Ok(());
        }
        if let Some(old) = old {
            st.events.append(&Event::NameReleased {
                ts: store::now(),
                agent: id.into(),
                name: old.clone(),
            })?;
            st.names.remove(&old);
        }
        st.events.append(&Event::NameClaimed {
            ts: store::now(),
            agent: id.into(),
            name: name.into(),
        })?;
        st.names.insert(name.into(), id.into());
        st.agents.get_mut(id).unwrap().name = Some(name.into());
        let _ = store::write_json(
            &self.agent_dir(id).join("name.json"),
            &json!({ "name": name, "claimed": store::now() }),
        );
        self.emit(Feed::NameClaimed {
            id: id.into(),
            name: name.into(),
        });
        Ok(())
    }

    /// Kill `target` and all its descendants. Only the user or an ancestor may kill.
    fn kill(&self, st: &mut State, from: &str, target: &str) -> Result<Vec<String>> {
        if target == HUMAN {
            bail!("cannot kill the user");
        }
        if from != HUMAN && !st.is_ancestor(from, target) {
            bail!(
                "only the user or an ancestor of {} can kill it",
                st.label(target)
            );
        }
        let mut doomed = vec![target.to_string()];
        let mut i = 0;
        while i < doomed.len() {
            let parent = doomed[i].clone();
            doomed.extend(
                st.agents
                    .values()
                    .filter(|a| a.meta.parent == parent)
                    .map(|a| a.meta.id.clone()),
            );
            i += 1;
        }
        let mut killed = Vec::new();
        for id in doomed {
            let Some(agent) = st.agents.get_mut(&id) else {
                continue;
            };
            if agent.killed {
                continue;
            }
            agent.killed = true;
            agent.pending.clear();
            if let Some(kill) = agent.proc.as_mut().and_then(|p| p.kill.take()) {
                let _ = kill.send(());
            }
            let name = agent.name.take();
            st.events.append(&Event::AgentKilled {
                ts: store::now(),
                agent: id.clone(),
                by: from.into(),
            })?;
            if let Some(name) = name {
                st.events.append(&Event::NameReleased {
                    ts: store::now(),
                    agent: id.clone(),
                    name: name.clone(),
                })?;
                st.names.remove(&name);
            }
            self.emit(Feed::AgentStatus {
                id: id.clone(),
                who: st.label(&id),
                status: Status::Killed,
            });
            killed.push(id);
        }
        Ok(killed)
    }

    fn stop_all(&self, st: &mut State) {
        st.shutting_down = true;
        for agent in st.agents.values_mut() {
            if let Some(kill) = agent.proc.as_mut().and_then(|p| p.kill.take()) {
                let _ = kill.send(());
            }
        }
    }

    /// Stop every pi process (they resume with `--continue` next time) and wait briefly.
    pub async fn stop(&self) {
        {
            let mut st = self.st.lock().unwrap();
            self.stop_all(&mut st);
        }
        for _ in 0..50 {
            if self
                .st
                .lock()
                .unwrap()
                .agents
                .values()
                .all(|a| a.proc.is_none())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    // ---------------------------------------------------------------- messages

    /// Append a message to a log and queue it for its recipients.
    fn post(
        &self,
        st: &mut State,
        from: &str,
        log: &str,
        text: String,
        kind: MsgKind,
    ) -> Result<(Message, Recipients)> {
        if from != HUMAN {
            let a = st.require_agent(from)?;
            if a.killed {
                bail!("agent {from} has been killed");
            }
        }
        if text.trim().is_empty() {
            bail!("message text is empty");
        }
        let mentions: Vec<String> = {
            let mut seen = BTreeSet::new();
            for cap in MENTION.captures_iter(&text) {
                if let Ok(id) = st.resolve_agent(&cap[1]) {
                    seen.insert(id);
                }
            }
            seen.into_iter().collect()
        };

        if !st.logs.contains_key(log) {
            let file = self
                .cfg
                .session
                .join("logs")
                .join(format!("{}.jsonl", log.replace(':', "_")));
            st.logs.insert(
                log.to_string(),
                Log {
                    file: Jsonl::open(&file, true)?,
                    msgs: Vec::new(),
                },
            );
        }
        let entry = st.logs.get(log).unwrap();
        let msg = Message {
            log: log.to_string(),
            seq: entry.msgs.len() as u64 + 1,
            ts: store::now(),
            from: from.to_string(),
            from_name: st.agents.get(from).and_then(|a| a.name.clone()),
            kind,
            mentions,
            text,
        };
        let entry = st.logs.get_mut(log).unwrap();
        entry.file.append(&msg)?;
        entry.msgs.push(msg.clone());

        let rcpt = st.recipients(&msg);
        for (id, &wake) in &rcpt {
            st.events.append(&Event::Routed {
                ts: store::now(),
                agent: id.clone(),
                log: msg.log.clone(),
                seq: msg.seq,
                wake,
            })?;
            let agent = st.agents.get_mut(id).unwrap();
            agent.pending.push(Pending {
                log: msg.log.clone(),
                seq: msg.seq,
                wake,
            });
            if wake {
                let now = Instant::now();
                agent.first_wake.get_or_insert(now);
                agent.last_wake = Some(now);
            }
        }
        let to = rcpt.keys().map(|id| st.label(id)).collect();
        self.emit(Feed::Message {
            msg: msg.clone(),
            title: st.log_title(log),
            who: st.label(from),
            to,
        });
        Ok((msg, rcpt))
    }

    // ---------------------------------------------------------------- delivery

    /// Deliver an agent's queued messages if any of them should wake it.
    ///
    /// Idle and dormant agents are debounced so a burst arrives as one turn; a dormant agent's
    /// pi process is started right away so it boots during the wait. Running agents get the
    /// messages immediately (pi queues them between turns).
    pub fn kick(self: &Arc<Self>, id: String) {
        {
            let mut st = self.st.lock().unwrap();
            let Some(agent) = st.agents.get_mut(&id) else {
                return;
            };
            if agent.debouncing || agent.killed || !agent.pending.iter().any(|p| p.wake) {
                return;
            }
            agent.debouncing = true;
            let dormant = agent.proc.is_none();
            if dormant
                && !st.shutting_down
                && let Err(e) = self.start_proc(&mut st, &id)
            {
                let who = st.label(&id);
                self.emit(Feed::Error {
                    who,
                    error: format!("starting agent failed: {e:#}"),
                });
            }
        }
        let me = self.clone();
        tokio::spawn(async move {
            loop {
                let wait = me.debounce_wait(&id);
                if wait.is_zero() {
                    break;
                }
                tokio::time::sleep(wait).await;
            }
            if let Some(agent) = me.st.lock().unwrap().agents.get_mut(&id) {
                agent.debouncing = false;
            }
            me.deliver(&id).await
        });
    }

    /// How much longer to hold an agent's queued messages.
    fn debounce_wait(&self, id: &str) -> Duration {
        let st = self.st.lock().unwrap();
        let Some(a) = st.agents.get(id) else {
            return Duration::ZERO;
        };
        let (Some(first), Some(last)) = (a.first_wake, a.last_wake) else {
            // Queued before a restart: nothing to wait for.
            return Duration::ZERO;
        };
        if a.urgent || a.running || self.cfg.debounce.is_zero() {
            return Duration::ZERO;
        }
        let deadline = (last + self.cfg.debounce).min(first + self.cfg.debounce_max);
        deadline.saturating_duration_since(Instant::now())
    }

    async fn deliver(self: &Arc<Self>, id: &str) {
        let prepared = {
            let mut st = self.st.lock().unwrap();
            self.prepare_delivery(&mut st, id)
        };
        let (rx, items) = match prepared {
            Ok(Some(p)) => p,
            Ok(None) => return,
            Err(e) => {
                let who = self.st.lock().unwrap().label(id);
                self.emit(Feed::Error {
                    who,
                    error: format!("delivery failed: {e:#}"),
                });
                return;
            }
        };
        let outcome = match tokio::time::timeout(DELIVERY_TIMEOUT, rx).await {
            Ok(Ok(resp)) if resp["success"] == true => Ok(()),
            Ok(Ok(resp)) => Err(resp["error"].as_str().unwrap_or("rejected").to_string()),
            Ok(Err(_)) => Err("agent process exited".to_string()),
            Err(_) => Err("timed out waiting for pi".to_string()),
        };
        let mut st = self.st.lock().unwrap();
        if let Some(agent) = st.agents.get_mut(id) {
            agent.inflight = agent.inflight.saturating_sub(1);
        }
        match outcome {
            Ok(()) => {
                let items = items.iter().map(|p| (p.log.clone(), p.seq)).collect();
                let ev = Event::Delivered {
                    ts: store::now(),
                    agent: id.into(),
                    items,
                };
                if let Err(e) = st.events.append(&ev) {
                    eprintln!("agentcord: {e:#}");
                }
            }
            Err(e) => {
                let who = st.label(id);
                if let Some(agent) = st.agents.get_mut(id).filter(|a| !a.killed) {
                    agent.pending.splice(0..0, items);
                }
                self.emit(Feed::Error {
                    who,
                    error: format!("delivery failed: {e}"),
                });
            }
        }
    }

    fn prepare_delivery(
        self: &Arc<Self>,
        st: &mut State,
        id: &str,
    ) -> Result<Option<(oneshot::Receiver<Value>, Vec<Pending>)>> {
        let Some(agent) = st.agents.get(id) else {
            return Ok(None);
        };
        if agent.killed || st.shutting_down || !agent.pending.iter().any(|p| p.wake) {
            return Ok(None);
        }
        if agent.proc.is_none() {
            self.start_proc(st, id)?;
        }
        let agent = st.agents.get_mut(id).unwrap();
        let items = std::mem::take(&mut agent.pending);
        agent.inflight += 1;
        agent.first_wake = None;
        agent.last_wake = None;
        agent.urgent = false;
        let text = st.render_delivery(id, &items);
        st.next_req += 1;
        let req_id = format!("ac-{}", st.next_req);
        let (tx, rx) = oneshot::channel();
        let proc = st.agents.get_mut(id).unwrap().proc.as_mut().unwrap();
        proc.waiters.insert(req_id.clone(), tx);
        // `steer` starts a run when idle and queues between turns when busy.
        let cmd = json!({ "id": req_id, "type": "prompt", "message": text, "streamingBehavior": "steer" });
        if proc.stdin.send(cmd).is_err() {
            proc.waiters.remove(&req_id);
            let agent = st.agents.get_mut(id).unwrap();
            agent.inflight -= 1;
            agent.pending.splice(0..0, items);
            bail!("agent stdin closed");
        }
        Ok(Some((rx, items)))
    }

    // ---------------------------------------------------------------- pi processes

    fn start_proc(self: &Arc<Self>, st: &mut State, id: &str) -> Result<()> {
        let agent = st.agents.get(id).unwrap();
        let meta = agent.meta.clone();
        let display = agent.name.clone().unwrap_or_else(|| id.to_string());
        let dir = self.agent_dir(id);
        let pi_dir = dir.join("pi");
        fs::create_dir_all(&pi_dir)?;
        let resumed = fs::read_dir(&pi_dir)?
            .filter_map(|e| e.ok())
            .any(|e| e.path().extension().is_some_and(|x| x == "jsonl"));

        let mut cmd = tokio::process::Command::new(&self.cfg.pi_bin);
        cmd.arg("--mode").arg("rpc");
        cmd.arg("--session-dir").arg(&pi_dir);
        cmd.arg("-e").arg(&self.ext_path);
        cmd.arg("--append-system-prompt").arg(dir.join("system.md"));
        cmd.arg("--name").arg(format!("agentcord {display}"));
        if resumed {
            cmd.arg("--continue");
        }
        if let Some(m) = meta.model.as_ref().or(self.cfg.model.as_ref()) {
            cmd.arg("--model").arg(m);
        }
        if let Some(t) = meta.thinking.as_ref().or(self.cfg.thinking.as_ref()) {
            cmd.arg("--thinking").arg(t);
        }
        if !self.cfg.user_extensions {
            cmd.arg("--no-extensions");
        }
        if self.cfg.no_builtin_tools {
            // Overrides any per-agent `tools` allowlist.
            cmd.arg("--no-builtin-tools")
                .arg("--exclude-tools")
                .arg(PI_BUILTIN_TOOLS);
        } else if let Some(tools) = &meta.tools {
            let mut all: Vec<&str> = tools
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .collect();
            all.extend(EXTENSION_TOOLS);
            cmd.arg("--tools").arg(all.join(","));
        }
        cmd.args(&self.cfg.pi_args);
        let stderr = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("stderr.log"))?;
        cmd.current_dir(&meta.cwd)
            .env("AGENTCORD_SOCKET", &self.socket)
            .env("AGENTCORD_AGENT_ID", id)
            .env("AGENTCORD_SESSION", &self.cfg.session)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::from(stderr))
            .kill_on_drop(true);
        let mut child = cmd
            .spawn()
            .with_context(|| format!("starting {}", self.cfg.pi_bin))?;
        let pid = child.id();
        let mut stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let rpc = self.rpc_file(id)?;

        st.next_gen += 1;
        let gen_ = st.next_gen;
        let (tx, mut rx) = mpsc::unbounded_channel::<Value>();
        // Deliver everything queued mid-run together after the current turn, not one per turn.
        let _ = tx.send(json!({ "type": "set_steering_mode", "mode": "all" }));
        let (kill_tx, kill_rx) = oneshot::channel::<()>();

        let log = rpc.clone();
        tokio::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                let rec =
                    json!({ "type": "agentcord_command", "ts": store::now(), "command": &cmd });
                let _ = log.lock().unwrap().append(&rec);
                let mut line = cmd.to_string();
                line.push('\n');
                if stdin.write_all(line.as_bytes()).await.is_err() || stdin.flush().await.is_err() {
                    break;
                }
            }
        });

        let me = self.clone();
        let aid = id.to_string();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let Ok(ev) = serde_json::from_str::<Value>(&line) else {
                    let _ = rpc
                        .lock()
                        .unwrap()
                        .append(&json!({ "type": "agentcord_stdout", "line": line }));
                    continue;
                };
                {
                    let mut file = rpc.lock().unwrap();
                    // Streaming deltas are noise on disk; message_end carries the full message.
                    if ev["type"] != "message_update" {
                        let _ = file.append_raw(&line);
                    }
                    if let Some(item) = flow::from_pi_event(&ev) {
                        let _ = me.flow.send((aid.clone(), item));
                    }
                }
                me.on_pi_event(&aid, gen_, ev);
            }
        });

        let me = self.clone();
        let aid = id.to_string();
        tokio::spawn(async move {
            let code = tokio::select! {
                status = child.wait() => status.ok().and_then(|s| s.code()),
                _ = kill_rx => { let _ = child.kill().await; None }
            };
            me.on_exit(&aid, gen_, code);
        });

        st.events.append(&Event::ProcessStarted {
            ts: store::now(),
            agent: id.into(),
            pid,
            resumed,
        })?;
        let agent = st.agents.get_mut(id).unwrap();
        agent.proc = Some(Proc {
            generation: gen_,
            stdin: tx,
            waiters: HashMap::new(),
            kill: Some(kill_tx),
        });
        agent.running = false;
        self.emit(Feed::AgentStatus {
            id: id.into(),
            who: st.label(id),
            status: Status::Idle,
        });
        Ok(())
    }

    fn on_pi_event(self: &Arc<Self>, id: &str, gen_: u64, ev: Value) {
        let mut kicks = Vec::new();
        {
            let mut st = self.st.lock().unwrap();
            let who = st.label(id);
            let Some(agent) = st.agents.get_mut(id) else {
                return;
            };
            let Some(proc) = agent.proc.as_mut().filter(|p| p.generation == gen_) else {
                return;
            };
            match ev["type"].as_str().unwrap_or("") {
                "response" => {
                    if let Some(tx) = ev["id"].as_str().and_then(|rid| proc.waiters.remove(rid)) {
                        let _ = tx.send(ev);
                    }
                }
                "extension_ui_request" => {
                    // Nobody is at a terminal to answer dialogs; dismiss them so the agent never blocks.
                    let method = ev["method"].as_str().unwrap_or("");
                    if matches!(method, "select" | "confirm" | "input" | "editor") {
                        let _ = proc.stdin.send(json!({ "type": "extension_ui_response", "id": ev["id"], "cancelled": true }));
                    }
                }
                "agent_start" => {
                    agent.running = true;
                    agent.final_text.clear();
                    self.emit(Feed::AgentStatus {
                        id: id.into(),
                        who,
                        status: Status::Running,
                    });
                }
                "message_end" if ev["message"]["role"] == "assistant" => {
                    let m = &ev["message"];
                    let text = assistant_text(m);
                    let answer = match m["stopReason"].as_str().unwrap_or("") {
                        "stop" if !text.trim().is_empty() => Some(text.clone()),
                        "length" => Some(format!("{text}\n\n[output truncated: length limit]")),
                        "error" => Some(format!(
                            "[run failed: {}]",
                            m["errorMessage"].as_str().unwrap_or("unknown error")
                        )),
                        _ => None,
                    };
                    agent.final_text.extend(answer);
                    if !text.trim().is_empty() {
                        self.emit(Feed::AssistantText {
                            id: id.into(),
                            who,
                            text,
                        });
                    }
                }
                "tool_execution_start" => {
                    let tool = ev["toolName"].as_str().unwrap_or("?").to_string();
                    let args = truncate(&ev["args"].to_string(), 240);
                    self.emit(Feed::ToolStart {
                        id: id.into(),
                        who,
                        tool,
                        args,
                    });
                }
                "tool_execution_end" => {
                    let tool = ev["toolName"].as_str().unwrap_or("?").to_string();
                    let is_error = ev["isError"].as_bool().unwrap_or(false);
                    self.emit(Feed::ToolEnd {
                        id: id.into(),
                        who,
                        tool,
                        is_error,
                    });
                }
                "extension_error" => {
                    let error = format!("extension error: {}", ev["error"].as_str().unwrap_or("?"));
                    self.emit(Feed::Error { who, error });
                }
                "agent_settled" => {
                    agent.running = false;
                    let answers = std::mem::take(&mut agent.final_text);
                    let report =
                        (agent.meta.report && !answers.is_empty()).then(|| answers.join("\n\n"));
                    let parent = agent.meta.parent.clone();
                    self.emit(Feed::AgentStatus {
                        id: id.into(),
                        who: who.clone(),
                        status: Status::Idle,
                    });
                    if let Some(text) = report {
                        let log = dm_log(id, &parent);
                        match self.post(&mut st, id, &log, text, MsgKind::Report) {
                            Ok((_, rcpt)) => collect_kicks(&rcpt, &mut kicks),
                            Err(e) => self.emit(Feed::Error {
                                who,
                                error: format!("report failed: {e:#}"),
                            }),
                        }
                    }
                    kicks.push(id.to_string());
                }
                _ => {}
            }
        }
        for id in kicks {
            self.kick(id);
        }
    }

    fn on_exit(self: &Arc<Self>, id: &str, gen_: u64, code: Option<i32>) {
        let mut kicks = Vec::new();
        {
            let mut st = self.st.lock().unwrap();
            let Some(agent) = st.agents.get_mut(id) else {
                return;
            };
            if agent.proc.as_ref().is_none_or(|p| p.generation != gen_) {
                return;
            }
            // Dropping the waiters fails any in-flight deliveries; they are re-queued.
            agent.proc = None;
            let was_running = std::mem::replace(&mut agent.running, false);
            let killed = agent.killed;
            let parent = agent.meta.parent.clone();
            let _ = st.events.append(&Event::ProcessExited {
                ts: store::now(),
                agent: id.into(),
                code,
            });
            let status = if killed {
                Status::Killed
            } else {
                Status::Dormant
            };
            let _ = self.flow.send((id.into(), FlowItem::Status { status }));
            self.emit(Feed::AgentStatus {
                id: id.into(),
                who: st.label(id),
                status,
            });
            if !killed && !st.shutting_down {
                let tail = stderr_tail(&self.agent_dir(id).join("stderr.log"));
                let text = format!(
                    "[agentcord] {} exited unexpectedly (code {}){}. It will be resumed from its session when it next receives a message.{}",
                    st.label(id),
                    code.map_or("none".into(), |c| c.to_string()),
                    if was_running {
                        " in the middle of a run"
                    } else {
                        ""
                    },
                    if tail.is_empty() {
                        String::new()
                    } else {
                        format!("\nstderr tail:\n{tail}")
                    },
                );
                if let Ok((_, rcpt)) =
                    self.post(&mut st, id, &dm_log(id, &parent), text, MsgKind::System)
                {
                    collect_kicks(&rcpt, &mut kicks);
                }
            }
        }
        for id in kicks {
            self.kick(id);
        }
    }
}

struct SpawnSpec {
    prompt: String,
    name: Option<String>,
    model: Option<String>,
    thinking: Option<String>,
    cwd: Option<String>,
    tools: Option<String>,
    system: Option<String>,
    report: Option<bool>,
}

impl Agent {
    fn new(meta: AgentMeta) -> Self {
        Self {
            meta,
            name: None,
            subs: BTreeMap::new(),
            pending: Vec::new(),
            killed: false,
            proc: None,
            running: false,
            inflight: 0,
            final_text: Vec::new(),
            first_wake: None,
            last_wake: None,
            urgent: false,
            debouncing: false,
        }
    }
}

impl State {
    fn replay(&mut self, ev: Event) {
        match ev {
            Event::AgentCreated { meta, .. } => {
                self.agents.insert(meta.id.clone(), Agent::new(meta));
            }
            Event::NameClaimed { agent, name, .. } => {
                if let Some(a) = self.agents.get_mut(&agent) {
                    if let Some(old) = a.name.replace(name.clone()) {
                        self.names.remove(&old);
                    }
                    self.names.insert(name, agent);
                }
            }
            Event::NameReleased { agent, name, .. } => {
                if self.names.get(&name) == Some(&agent) {
                    self.names.remove(&name);
                }
                if let Some(a) = self
                    .agents
                    .get_mut(&agent)
                    .filter(|a| a.name.as_ref() == Some(&name))
                {
                    a.name = None;
                }
            }
            Event::TopicCreated { meta, .. } => {
                self.topics
                    .insert(meta.id.clone(), Topic { meta, name: None });
            }
            Event::TopicNamed { topic, name, .. } => self.rename_topic(&topic, &name),
            Event::Subscribed {
                agent, topic, mode, ..
            } => {
                if let Some(a) = self.agents.get_mut(&agent) {
                    a.subs.insert(topic, mode);
                }
            }
            Event::Unsubscribed { agent, topic, .. } => {
                if let Some(a) = self.agents.get_mut(&agent) {
                    a.subs.remove(&topic);
                }
            }
            Event::Routed {
                agent,
                log,
                seq,
                wake,
                ..
            } => {
                if let Some(a) = self.agents.get_mut(&agent) {
                    a.pending.push(Pending { log, seq, wake });
                }
            }
            Event::Delivered { agent, items, .. } => {
                if let Some(a) = self.agents.get_mut(&agent) {
                    a.pending
                        .retain(|p| !items.iter().any(|(l, s)| *l == p.log && *s == p.seq));
                }
            }
            Event::AgentKilled { agent, .. } => {
                if let Some(a) = self.agents.get_mut(&agent) {
                    a.killed = true;
                    a.pending.clear();
                }
            }
            Event::ProcessStarted { .. } | Event::ProcessExited { .. } => {}
        }
    }

    fn require_agent(&self, id: &str) -> Result<&Agent> {
        self.agents
            .get(id)
            .ok_or_else(|| anyhow!("unknown agent {id}"))
    }

    fn label(&self, id: &str) -> String {
        if id == HUMAN {
            return format!("@{HUMAN}");
        }
        match self.agents.get(id).and_then(|a| a.name.as_ref()) {
            Some(name) => format!("@{name} ({id})"),
            None => id.to_string(),
        }
    }

    fn check_name(&self, name: &str, claimant: Option<&str>) -> Result<()> {
        if name == HUMAN || name == LEGACY_HUMAN {
            bail!("@{name} is reserved for the human operator");
        }
        if !NAME.is_match(name) || ID_LIKE.is_match(name) {
            bail!(
                "invalid name {name:?}: use 1-32 chars of a-z, 0-9, '_' or '-', starting with a letter"
            );
        }
        match self.names.get(name) {
            Some(holder) if Some(holder.as_str()) != claimant => {
                bail!("name @{name} is already claimed by {}", self.label(holder))
            }
            _ => Ok(()),
        }
    }

    fn check_topic_name(&self, name: &str, topic: Option<&str>) -> Result<()> {
        if !NAME.is_match(name) || ID_LIKE.is_match(name) {
            bail!(
                "invalid topic name {name:?}: use 1-32 chars of a-z, 0-9, '_' or '-', starting with a letter"
            );
        }
        match self.topic_names.get(name) {
            Some(holder) if Some(holder.as_str()) != topic => {
                bail!("topic name #{name} is taken by {holder}")
            }
            _ => Ok(()),
        }
    }

    fn rename_topic(&mut self, id: &str, name: &str) {
        let Some(t) = self.topics.get_mut(id) else {
            return;
        };
        if let Some(old) = t.name.replace(name.to_string()) {
            self.topic_names.remove(&old);
        }
        self.topic_names.insert(name.to_string(), id.to_string());
    }

    fn topic_label(&self, id: &str) -> String {
        match self.topics.get(id).and_then(|t| t.name.as_ref()) {
            Some(name) => format!("#{name} ({id})"),
            None => id.to_string(),
        }
    }

    /// Short human form of a log key: `#name` for topics, `DM a ↔ b` for DMs.
    fn log_title(&self, log: &str) -> String {
        match dm_participants(log) {
            Some([a, b]) => format!("DM {} ↔ {}", self.label(a), self.label(b)),
            None => self.topic_label(log),
        }
    }

    /// `#name`, `name` or a topic id.
    fn resolve_topic(&self, spec: &str) -> Result<String> {
        let spec = spec.trim().trim_start_matches('#');
        if self.topics.contains_key(spec) {
            return Ok(spec.into());
        }
        self.topic_names
            .get(spec)
            .cloned()
            .ok_or_else(|| anyhow!("no topic named or with id {spec:?} (see topic_list)"))
    }

    /// `@name`, `name`, an agent id, or `human`.
    fn resolve_agent(&self, spec: &str) -> Result<String> {
        let spec = spec.trim().trim_start_matches('@');
        if spec == HUMAN || spec == LEGACY_HUMAN {
            return Ok(HUMAN.into());
        }
        if self.agents.contains_key(spec) {
            return Ok(spec.into());
        }
        self.names
            .get(spec)
            .cloned()
            .ok_or_else(|| anyhow!("no agent named or with id {spec:?}"))
    }

    fn is_ancestor(&self, ancestor: &str, id: &str) -> bool {
        let mut cur = id.to_string();
        for _ in 0..10_000 {
            let Some(a) = self.agents.get(&cur) else {
                return false;
            };
            if a.meta.parent == ancestor {
                return true;
            }
            cur = a.meta.parent.clone();
        }
        false
    }

    /// `@agent` → the DM log with that agent, `dm:...` as is, otherwise a topic.
    fn resolve_log(&self, from: &str, spec: &str) -> Result<String> {
        let spec = spec.trim();
        if spec.starts_with('@') {
            let other = self.resolve_agent(spec)?;
            if other == from {
                bail!("cannot DM yourself");
            }
            return Ok(dm_log(from, &other));
        }
        if spec.starts_with("dm:") {
            return if self.logs.contains_key(spec) {
                Ok(spec.into())
            } else {
                bail!("no such log {spec}")
            };
        }
        self.resolve_topic(spec)
    }

    fn recipients(&self, msg: &Message) -> Recipients {
        let mut rcpt = Recipients::new();
        let add = |rcpt: &mut Recipients, id: &str, wake: bool| {
            let e = rcpt.entry(id.to_string()).or_insert(wake);
            *e |= wake;
        };
        if let Some(parts) = dm_participants(&msg.log) {
            for p in parts {
                add(&mut rcpt, p, true);
            }
        } else {
            for a in self.agents.values() {
                if let Some(mode) = a.subs.get(&msg.log) {
                    add(&mut rcpt, &a.meta.id, *mode == SubMode::Wake);
                }
            }
        }
        for id in &msg.mentions {
            add(&mut rcpt, id, true);
        }
        rcpt.retain(|id, _| id != &msg.from && self.agents.get(id).is_some_and(|a| !a.killed));
        rcpt
    }

    fn message(&self, log: &str, seq: u64) -> Option<&Message> {
        self.logs.get(log)?.msgs.get((seq as usize).checked_sub(1)?)
    }

    fn render_delivery(&self, me: &str, items: &[Pending]) -> String {
        let n = items.len();
        let mut out = format!(
            "[agentcord] {n} new message{} for you ({}):\n",
            if n == 1 { "" } else { "s" },
            self.label(me)
        );
        let parent = self
            .agents
            .get(me)
            .map(|a| a.meta.parent.as_str())
            .unwrap_or("");
        for p in items {
            let Some(m) = self.message(&p.log, p.seq) else {
                continue;
            };
            let from = self.label(&m.from);
            let what = match (m.kind, m.log.starts_with("dm:")) {
                (MsgKind::Task, _) => format!("TASK from {from} (your parent)"),
                (MsgKind::Report, _) => format!("REPORT from {from} (an agent you spawned)"),
                (MsgKind::System, _) => format!("NOTICE from {from}"),
                (_, true) if m.from == parent => format!("DM from {from} (your parent)"),
                (_, true) => format!("DM from {from}"),
                (_, false) => format!("post in topic {} from {from}", self.topic_label(&m.log)),
            };
            let mention = if !m.log.starts_with("dm:") && m.mentions.iter().any(|x| x == me) {
                " · mentions you"
            } else {
                ""
            };
            let digest = if p.wake { "" } else { " · digest" };
            out.push_str(&format!(
                "\n── {what} · [{}#{}]{mention}{digest} · {}\n{}\n",
                m.log, m.seq, m.ts, m.text
            ));
        }
        out.push_str(
            "\n(Reply to DMs with send_message and to topics with post(topic, text). Your final response goes to \
             your parent as a report. If nothing needs doing, call wait_for_messages.)",
        );
        out
    }

    fn read_window(
        &self,
        log: &str,
        before: Option<u64>,
        after: Option<u64>,
        limit: Option<usize>,
    ) -> Response {
        let limit = limit.unwrap_or(20).clamp(1, 200);
        let all: &[Message] = self.logs.get(log).map(|l| l.msgs.as_slice()).unwrap_or(&[]);
        let window: &[Message] = match after {
            Some(a) => {
                let start = (a as usize).min(all.len());
                &all[start..(start + limit).min(all.len())]
            }
            None => {
                let end =
                    before.map_or(all.len(), |b| (b as usize).saturating_sub(1).min(all.len()));
                &all[end.saturating_sub(limit)..end]
            }
        };
        let title = self.log_title(log);
        let total = all.len();
        if window.is_empty() {
            return Response::ok(
                format!("{title}: no messages in that window ({total} total)."),
                json!({ "log": log, "messages": [], "total": total }),
            );
        }
        let (first, last) = (window[0].seq, window[window.len() - 1].seq);
        let mut text = format!(
            "{title} — {} of {total} message(s), seq {first}..{last}",
            window.len()
        );
        if first > 1 {
            text.push_str(&format!(" · older: read_log(before={first})"));
        }
        if (last as usize) < total {
            text.push_str(&format!(" · newer: read_log(after={last})"));
        }
        text.push('\n');
        for m in window {
            let kind = match m.kind {
                MsgKind::Message => String::new(),
                k => format!(" [{}]", serde_json::to_value(k).unwrap().as_str().unwrap()),
            };
            text.push_str(&format!(
                "\n[{}#{} · {} · {}{kind}]\n{}\n",
                m.log,
                m.seq,
                m.ts,
                self.label(&m.from),
                m.text
            ));
        }
        Response::ok(
            text,
            json!({ "log": log, "messages": window, "total": total }),
        )
    }

    fn snapshot(&self, session: &Path) -> Snapshot {
        let agents = self
            .agents
            .values()
            .map(|a| AgentInfo {
                id: a.meta.id.clone(),
                name: a.name.clone(),
                label: self.label(&a.meta.id),
                parent: a.meta.parent.clone(),
                created: a.meta.created.clone(),
                status: a.status(),
                pending: a.pending.len(),
                model: a.meta.model.clone(),
                cwd: a.meta.cwd.clone(),
                task: self
                    .message(&dm_log(&a.meta.parent, &a.meta.id), 1)
                    .map(|m| m.text.clone())
                    .unwrap_or_default(),
                subscriptions: a.subs.clone(),
            })
            .collect();
        let mut logs: Vec<LogInfo> = self
            .topics
            .values()
            .map(|t| {
                let msgs = self
                    .logs
                    .get(&t.meta.id)
                    .map(|l| l.msgs.as_slice())
                    .unwrap_or(&[]);
                LogInfo {
                    log: t.meta.id.clone(),
                    kind: LogKind::Topic,
                    title: self.topic_label(&t.meta.id),
                    name: t.name.clone(),
                    description: t.meta.description.clone(),
                    count: msgs.len(),
                    last: msgs.last().map(|m| m.ts.clone()),
                    members: self
                        .agents
                        .values()
                        .filter(|a| !a.killed && a.subs.contains_key(&t.meta.id))
                        .map(|a| self.label(&a.meta.id))
                        .collect(),
                }
            })
            .collect();
        for (key, log) in &self.logs {
            let Some(parts) = dm_participants(key) else {
                continue;
            };
            logs.push(LogInfo {
                log: key.clone(),
                kind: LogKind::Dm,
                title: self.log_title(key),
                name: None,
                description: None,
                count: log.msgs.len(),
                last: log.msgs.last().map(|m| m.ts.clone()),
                members: parts.iter().map(|p| self.label(p)).collect(),
            });
        }
        Snapshot {
            session: session.display().to_string(),
            agents,
            logs,
        }
    }

    fn list_agents(&self, from: &str) -> Response {
        let mut text = String::new();
        let mut rows = Vec::new();
        for a in self.agents.values() {
            let id = &a.meta.id;
            let task = self
                .message(&dm_log(&a.meta.parent, id), 1)
                .map(|m| truncate(&m.text.replace('\n', " "), 80))
                .unwrap_or_default();
            let status = a.status();
            let you = if id == from { " (you)" } else { "" };
            let subs: Vec<String> = a.subs.keys().map(|t| self.topic_label(t)).collect();
            text.push_str(&format!(
                "{}{you} · {} · parent {} · {} queued{}\n    task: {task}\n",
                self.label(id),
                serde_json::to_value(status).unwrap().as_str().unwrap(),
                self.label(&a.meta.parent),
                a.pending.len(),
                if subs.is_empty() {
                    String::new()
                } else {
                    format!(" · subscribed {}", subs.join(" "))
                },
            ));
            rows.push(json!({
                "id": id, "name": a.name, "parent": a.meta.parent, "status": status,
                "pending": a.pending.len(), "subscriptions": a.subs, "model": a.meta.model,
                "cwd": a.meta.cwd, "task": task,
            }));
        }
        if text.is_empty() {
            text = "No agents.".into();
        }
        Response::ok(text, json!({ "agents": rows }))
    }

    fn list_topics(&self, from: &str) -> Response {
        let mut text = String::new();
        let mut rows = Vec::new();
        let mine = self.agents.get(from).map(|a| &a.subs);
        for (id, topic) in &self.topics {
            let msgs = self.logs.get(id).map(|l| l.msgs.as_slice()).unwrap_or(&[]);
            let subs: Vec<String> = self
                .agents
                .values()
                .filter(|a| !a.killed && a.subs.contains_key(id))
                .map(|a| self.label(&a.meta.id))
                .collect();
            let last = msgs.last().map(|m| m.ts.clone()).unwrap_or_default();
            let you = mine
                .and_then(|s| s.get(id))
                .map(|m| format!(" · YOU ARE SUBSCRIBED ({})", mode_str(*m)));
            text.push_str(&format!(
                "{}{} · {} message(s){} · created by {}\n",
                self.topic_label(id),
                you.clone().unwrap_or_default(),
                msgs.len(),
                if last.is_empty() {
                    String::new()
                } else {
                    format!(" · last {last}")
                },
                self.label(&topic.meta.created_by),
            ));
            if let Some(d) = &topic.meta.description {
                text.push_str(&format!("    {d}\n"));
            }
            text.push_str(&format!(
                "    subscribers: {}\n",
                if subs.is_empty() {
                    "none".to_string()
                } else {
                    subs.join(", ")
                }
            ));
            rows.push(json!({
                "id": id, "name": topic.name, "description": topic.meta.description,
                "created_by": topic.meta.created_by, "count": msgs.len(), "last": last,
                "subscribers": subs, "subscribed": mine.and_then(|s| s.get(id)),
            }));
        }
        if text.is_empty() {
            text = "No topics yet. Create one with topic_create or by posting to a new #name.\n"
                .into();
        }
        let dms: Vec<String> = self
            .logs
            .iter()
            .filter(|(k, _)| dm_participants(k).is_some_and(|p| p.contains(&from)))
            .map(|(k, l)| {
                format!(
                    "{} ({} message(s)) — read_log(\"{k}\")",
                    self.log_title(k),
                    l.msgs.len()
                )
            })
            .collect();
        if !dms.is_empty() {
            text.push_str(&format!(
                "\nYour DM conversations:\n  {}\n",
                dms.join("\n  ")
            ));
        }
        Response::ok(text, json!({ "topics": rows, "dms": dms }))
    }
}

fn mode_str(mode: SubMode) -> &'static str {
    match mode {
        SubMode::Wake => "wake",
        SubMode::Digest => "digest",
    }
}

fn collect_kicks(rcpt: &Recipients, kicks: &mut Vec<String>) {
    kicks.extend(
        rcpt.iter()
            .filter(|(_, wake)| **wake)
            .map(|(id, _)| id.clone()),
    );
}

fn sent_text(st: &State, msg: &Message, rcpt: &Recipients) -> String {
    let to: Vec<String> = rcpt
        .iter()
        .map(|(id, wake)| format!("{}{}", st.label(id), if *wake { "" } else { " (digest)" }))
        .collect();
    let to_human = msg.log.starts_with("dm:") && msg.log.contains(HUMAN) && msg.from != HUMAN;
    let delivered = if to_human {
        format!("delivered to @{HUMAN}")
    } else if to.is_empty() {
        "no agent recipients".to_string()
    } else {
        format!("delivered to {}", to.join(", "))
    };
    format!(
        "Posted {} as {}#{} ({delivered}).",
        st.log_title(&msg.log),
        msg.log,
        msg.seq
    )
}

pub fn dm_log(a: &str, b: &str) -> String {
    let (x, y) = if a <= b { (a, b) } else { (b, a) };
    format!("dm:{x}+{y}")
}

fn dm_participants(log: &str) -> Option<[&str; 2]> {
    let (a, b) = log.strip_prefix("dm:")?.split_once('+')?;
    Some([a, b])
}

fn new_agent_id(st: &State) -> String {
    loop {
        let id = format!("a-{}", &uuid::Uuid::new_v4().simple().to_string()[..8]);
        if !st.agents.contains_key(&id) {
            return id;
        }
    }
}

fn assistant_text(message: &Value) -> String {
    message["content"]
        .as_array()
        .map(|blocks| {
            blocks
                .iter()
                .filter(|b| b["type"] == "text")
                .filter_map(|b| b["text"].as_str())
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default()
}

pub fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let cut: String = s.chars().take(max).collect();
    format!("{cut}…")
}

fn stderr_tail(path: &Path) -> String {
    let Ok(s) = fs::read_to_string(path) else {
        return String::new();
    };
    let lines: Vec<&str> = s.lines().collect();
    lines[lines.len().saturating_sub(8)..].join("\n")
}

fn system_prompt(st: &State, meta: &AgentMeta, name: Option<&str>) -> String {
    let me = match name {
        Some(n) => format!("`{}` (name @{n})", meta.id),
        None => format!("`{}`", meta.id),
    };
    let parent = if meta.parent == HUMAN {
        "the human operator (`@human`)".to_string()
    } else {
        format!("agent {}", st.label(&meta.parent))
    };
    let report = if meta.report {
        format!(
            "- When a run ends, your final response is delivered to {parent} as your report. Put results in your final \
             response rather than also sending them to your parent with send_message."
        )
    } else {
        "- Your final response is NOT forwarded to anyone. Use send_message or post to deliver results.".to_string()
    };
    let extra = meta
        .system
        .as_deref()
        .map(|s| format!("\n{s}\n"))
        .unwrap_or_default();
    format!(
        "# agentcord\n\n\
You are agent {me} in an agentcord multi-agent session, spawned by {parent}.\n\n\
Other agents can message you and you can message them with the agentcord tools \
(agent_spawn, agent_list, agent_kill, claim_name, send_message, topic_create, topic_name, topic_invite, topic_list, subscribe, unsubscribe, post, \
read_log, wait_for_messages).\n\n\
- Messages for you arrive as user turns beginning with `[agentcord]`. They come from other agents unless the sender is `@human`, the human operator. You can message the human with send_message(to=\"@human\").\n\
- You are woken, and messages are inserted into your context, for: DMs to you, posts in topics you are subscribed to (mode \"wake\", \
the default), @-mentions of you anywhere, and reports from agents you spawned. Posts in topics subscribed with mode \"digest\" \
ride along with your next wake-up instead of waking you.\n\
{report}\n\
- To wait for other agents (for example children you spawned), call wait_for_messages. It ends your turn silently (no report) \
and you are woken when a message arrives. Do not poll read_log in a loop. If you are woken by something that needs no action \
from you, call wait_for_messages instead of writing a reply like \"no action needed\" (that would be sent as a report).\n\
- Reply to agents other than your parent with send_message. Do not reply to messages that need no answer; avoid acknowledgement ping-pong.\n\
- Topics are durable, named, pub/sub conversations (like threads or channels) addressed as `#name` or by id `t-...`. \
Create one with topic_create (optionally inviting agents), name or rename it with topic_name, and see all topics and your \
subscriptions with topic_list. Posting to a topic subscribes you to it; unsubscribe to stop being woken by it. Posting to a new \
`#name` creates that topic.\n\
- Every topic and DM conversation is an ordered log; messages are addressed `log#seq`. read_log(log, before/after/limit) scrolls \
through history; read a DM conversation with read_log(\"@name\").\n\
- Claim a unique name with claim_name so others can address you as @name.\n\
- Spawned agents inherit your working directory and model unless you override them. Give each a self-contained task.\n{extra}"
    )
}
