//! Wire and storage types shared by the agentcord daemon, its socket and HTTP/WebSocket APIs,
//! the CLI and the TUI.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// The human operator's identity. It is reserved: no agent can claim it, and `@human`
/// addresses the operator (DMs to it are logged for the TUI/CLI to show).
pub const HUMAN: &str = "human";
/// Older name for the human, still accepted when addressing.
pub const LEGACY_HUMAN: &str = "user";

/// One entry in a log (a topic or a DM conversation). Its address is `log#seq`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    pub log: String,
    pub seq: u64,
    pub ts: String,
    /// Agent id, or `human`.
    pub from: String,
    /// The sender's claimed name at send time, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_name: Option<String>,
    #[serde(default)]
    pub kind: MsgKind,
    /// Agent ids mentioned with `@name` / `@id`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mentions: Vec<String>,
    pub text: String,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MsgKind {
    #[default]
    Message,
    /// The task a parent gave when spawning.
    Task,
    /// A child's final answer, posted automatically when its run settles.
    Report,
    /// Harness notices (agent exited, killed, ...).
    System,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubMode {
    /// Every new message wakes the subscriber.
    Wake,
    /// Messages queue up and ride along with the subscriber's next wake.
    Digest,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentMeta {
    pub id: String,
    pub parent: String,
    pub created: String,
    pub cwd: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tools: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system: Option<String>,
    /// Post the final text of each run to the parent as a `report`.
    pub report: bool,
}

/// A named, subscribable log. Like agents, topics have a stable id and an optional unique name.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TopicMeta {
    pub id: String,
    pub created: String,
    pub created_by: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Broker state changes. `events.jsonl` is replayed on startup to rebuild the session.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    AgentCreated {
        ts: String,
        meta: AgentMeta,
    },
    NameClaimed {
        ts: String,
        agent: String,
        name: String,
    },
    NameReleased {
        ts: String,
        agent: String,
        name: String,
    },
    TopicCreated {
        ts: String,
        meta: TopicMeta,
    },
    TopicNamed {
        ts: String,
        topic: String,
        name: String,
        by: String,
    },
    Subscribed {
        ts: String,
        agent: String,
        topic: String,
        mode: SubMode,
        by: String,
    },
    Unsubscribed {
        ts: String,
        agent: String,
        topic: String,
    },
    /// A message was queued for an agent's context.
    Routed {
        ts: String,
        agent: String,
        log: String,
        seq: u64,
        wake: bool,
    },
    /// Queued messages were accepted into the agent's pi session.
    Delivered {
        ts: String,
        agent: String,
        items: Vec<(String, u64)>,
    },
    ProcessStarted {
        ts: String,
        agent: String,
        pid: Option<u32>,
        resumed: bool,
    },
    ProcessExited {
        ts: String,
        agent: String,
        code: Option<i32>,
    },
    AgentKilled {
        ts: String,
        agent: String,
        by: String,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    /// No process; it is started when a waking message arrives.
    Dormant,
    Idle,
    Running,
    Killed,
}

/// Live activity stream for `watch` / `run`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Feed {
    Message {
        msg: Message,
        title: String,
        who: String,
        to: Vec<String>,
    },
    AgentCreated {
        id: String,
        who: String,
        parent: String,
    },
    AgentStatus {
        id: String,
        who: String,
        status: Status,
    },
    AssistantText {
        id: String,
        who: String,
        text: String,
    },
    ToolStart {
        id: String,
        who: String,
        tool: String,
        args: String,
    },
    ToolEnd {
        id: String,
        who: String,
        tool: String,
        is_error: bool,
    },
    NameClaimed {
        id: String,
        name: String,
    },
    TopicCreated {
        id: String,
        topic: String,
        by: String,
    },
    TopicNamed {
        id: String,
        name: String,
        by: String,
    },
    Subscribed {
        who: String,
        topic: String,
        mode: SubMode,
    },
    Unsubscribed {
        who: String,
        topic: String,
    },
    Error {
        who: String,
        error: String,
    },
}

fn human() -> String {
    HUMAN.to_string()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    Spawn {
        #[serde(default = "human")]
        from: String,
        prompt: String,
        name: Option<String>,
        model: Option<String>,
        thinking: Option<String>,
        cwd: Option<String>,
        tools: Option<String>,
        system: Option<String>,
        report: Option<bool>,
    },
    Send {
        #[serde(default = "human")]
        from: String,
        to: String,
        text: String,
    },
    /// Post to a topic (`#name` or id). Posting to an unknown `#name` creates it.
    Post {
        #[serde(default = "human")]
        from: String,
        topic: String,
        text: String,
    },
    TopicCreate {
        #[serde(default = "human")]
        from: String,
        name: Option<String>,
        description: Option<String>,
        /// Agents to subscribe (with wake) besides the creator.
        #[serde(default)]
        invite: Vec<String>,
    },
    /// Subscribe other agents to a topic (mode wake).
    Invite {
        #[serde(default = "human")]
        from: String,
        topic: String,
        agents: Vec<String>,
    },
    TopicName {
        #[serde(default = "human")]
        from: String,
        topic: String,
        name: String,
    },
    Topics {
        #[serde(default = "human")]
        from: String,
    },
    ClaimName {
        from: String,
        name: String,
    },
    Subscribe {
        from: String,
        topic: String,
        mode: Option<SubMode>,
    },
    Unsubscribe {
        from: String,
        topic: String,
    },
    Read {
        #[serde(default = "human")]
        from: String,
        log: String,
        before: Option<u64>,
        after: Option<u64>,
        limit: Option<usize>,
    },
    Agents {
        #[serde(default = "human")]
        from: String,
    },
    /// Everything a UI needs to draw: all agents and all logs (topics and DMs).
    State,
    Kill {
        #[serde(default = "human")]
        from: String,
        agent: String,
    },
    Watch,
    Shutdown,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Response {
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Human/LLM-readable rendering of the result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl Response {
    pub fn ok(text: impl Into<String>, data: Value) -> Self {
        Self {
            ok: true,
            error: None,
            text: Some(text.into()),
            data: Some(data),
        }
    }
    pub fn err(e: impl std::fmt::Display) -> Self {
        Self {
            ok: false,
            error: Some(e.to_string()),
            ..Default::default()
        }
    }
}

/// Response data of `Request::State` / `GET /api/state`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Snapshot {
    pub session: String,
    pub agents: Vec<AgentInfo>,
    pub logs: Vec<LogInfo>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentInfo {
    pub id: String,
    pub name: Option<String>,
    /// `@name (id)` or `id`.
    pub label: String,
    pub parent: String,
    pub created: String,
    pub status: Status,
    /// Messages routed to it and not yet in its context.
    pub pending: usize,
    pub model: Option<String>,
    pub cwd: String,
    pub task: String,
    /// topic id -> mode
    pub subscriptions: BTreeMap<String, SubMode>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogKind {
    Topic,
    Dm,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogInfo {
    /// Log key: a topic id (`t-...`) or `dm:<a>+<b>`.
    pub log: String,
    pub kind: LogKind,
    /// `#name (t-...)` or `DM @a (..) ↔ @b (..)`.
    pub title: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub count: usize,
    pub last: Option<String>,
    /// Topic subscribers, or the two DM participants (labels).
    pub members: Vec<String>,
}

/// One step of an agent's own flow: what enters its context and what it does.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FlowItem {
    Status {
        status: Status,
    },
    /// A user turn inserted into the agent's context (agentcord messages, tasks).
    Inbound {
        text: String,
    },
    /// Live streaming chunk of the assistant's text or thinking. Not replayed in backlogs.
    Delta {
        thinking: bool,
        delta: String,
    },
    /// A completed assistant message.
    Assistant {
        text: String,
        thinking: String,
        tool_calls: Vec<ToolCall>,
        stop_reason: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    ToolStart {
        call_id: String,
        tool: String,
        args: String,
    },
    ToolEnd {
        call_id: String,
        tool: String,
        result: String,
        is_error: bool,
    },
    /// Compaction, retries and other harness notes.
    Note {
        text: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub args: String,
}

/// Frames sent on the WebSocket (`GET /ws`). Subscriptions that have history send it first
/// (`live: false`), then `synced`, then live frames.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WsFrame {
    /// `/ws` with no filter: the global activity feed.
    Feed {
        event: Feed,
    },
    /// `/ws?agent=...`: that agent's flow.
    Flow {
        agent: String,
        live: bool,
        item: FlowItem,
    },
    /// `/ws?log=...`: messages of one topic or DM log.
    Message {
        live: bool,
        who: String,
        msg: Message,
    },
    Synced,
    Error {
        error: String,
    },
}
