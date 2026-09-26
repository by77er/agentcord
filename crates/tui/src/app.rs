//! TUI state, input handling and reactions to daemon events.

use crate::net::Api;
use agentcord_proto::{
    AgentInfo, Feed, FlowItem, HUMAN, LogInfo, LogKind, Message, Snapshot, WsFrame,
};
use ratatui::crossterm::event::{Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::style::Color;
use serde_json::json;
use std::collections::{HashMap, VecDeque};
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;

const MAX_ENTRIES: usize = 3000;
const MAX_ACTIVITY: usize = 400;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Focus {
    Agents,
    Logs,
    Input,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ViewKind {
    Agent(String),
    Log(String),
}

pub enum Entry {
    Flow(FlowItem),
    Msg { who: String, msg: Message },
}

pub struct View {
    pub kind: ViewKind,
    generation: u64,
    pub entries: VecDeque<Entry>,
    /// Streaming assistant output not yet finalized by an `assistant` item.
    pub stream_text: String,
    pub stream_thinking: String,
    pub synced: bool,
    pub error: Option<String>,
}

/// Which WebSocket a frame came from.
#[derive(Clone, Copy)]
pub enum Tag {
    Feed,
    View(u64),
}

pub enum AppEvent {
    Snapshot(Snapshot),
    Info(String),
    Error(String),
    Open(ViewKind),
}

pub struct App {
    pub api: Api,
    pub snap: Snapshot,
    pub focus: Focus,
    pub agent_sel: usize,
    pub log_sel: usize,
    pub view: Option<View>,
    view_task: Option<JoinHandle<()>>,
    next_gen: u64,
    pub input: String,
    /// Cursor position in chars.
    pub cursor: usize,
    pub activity: VecDeque<(String, Color, String)>,
    pub status: (String, bool),
    pub unread: HashMap<String, usize>,
    /// Lines scrolled up from the bottom of the view (0 = follow).
    pub scroll: usize,
    pub view_height: usize,
    pub show_help: bool,
    pub quit: bool,
    state_dirty: bool,
    fetching: bool,
    ws_tx: UnboundedSender<(Tag, WsFrame)>,
    ev_tx: UnboundedSender<AppEvent>,
}

impl App {
    pub fn new(
        api: Api,
        ws_tx: UnboundedSender<(Tag, WsFrame)>,
        ev_tx: UnboundedSender<AppEvent>,
    ) -> Self {
        Self {
            api,
            snap: Snapshot::default(),
            focus: Focus::Agents,
            agent_sel: 0,
            log_sel: 0,
            view: None,
            view_task: None,
            next_gen: 0,
            input: String::new(),
            cursor: 0,
            activity: VecDeque::new(),
            status: ("? for help".into(), false),
            unread: HashMap::new(),
            scroll: 0,
            view_height: 20,
            show_help: false,
            quit: false,
            state_dirty: true,
            fetching: false,
            ws_tx,
            ev_tx,
        }
    }

    // ------------------------------------------------------------ derived lists

    /// Agents in tree order (children under their parent) with their depth.
    pub fn agent_rows(&self) -> Vec<(usize, &AgentInfo)> {
        let mut agents: Vec<&AgentInfo> = self.snap.agents.iter().collect();
        agents.sort_by(|a, b| a.created.cmp(&b.created));
        let known = |id: &str| agents.iter().any(|a| a.id == id);
        let mut out = Vec::new();
        fn walk<'a>(
            parent: &str,
            depth: usize,
            all: &[&'a AgentInfo],
            out: &mut Vec<(usize, &'a AgentInfo)>,
        ) {
            for a in all.iter().filter(|a| a.parent == parent) {
                out.push((depth, *a));
                walk(&a.id, depth + 1, all, out);
            }
        }
        for root in agents.iter().filter(|a| !known(&a.parent)) {
            out.push((0, *root));
            walk(&root.id, 1, &agents, &mut out);
        }
        out
    }

    /// Topics (by name) then DMs (most recent first).
    pub fn log_rows(&self) -> Vec<&LogInfo> {
        let mut topics: Vec<&LogInfo> = self
            .snap
            .logs
            .iter()
            .filter(|l| l.kind == LogKind::Topic)
            .collect();
        topics.sort_by(|a, b| a.title.cmp(&b.title));
        let mut dms: Vec<&LogInfo> = self
            .snap
            .logs
            .iter()
            .filter(|l| l.kind == LogKind::Dm)
            .collect();
        dms.sort_by(|a, b| b.last.cmp(&a.last));
        topics.extend(dms);
        topics
    }

    pub fn agent(&self, id: &str) -> Option<&AgentInfo> {
        self.snap.agents.iter().find(|a| a.id == id)
    }

    pub fn log(&self, key: &str) -> Option<&LogInfo> {
        self.snap.logs.iter().find(|l| l.log == key)
    }

    /// Unread messages addressed to the human (DMs with @human).
    pub fn inbox(&self) -> usize {
        self.unread
            .iter()
            .filter(|(log, _)| is_human_dm(log))
            .map(|(_, n)| n)
            .sum()
    }

    // ------------------------------------------------------------ events

    pub fn on_term(&mut self, ev: Event) {
        let Event::Key(key) = ev else { return };
        if key.kind != KeyEventKind::Press {
            return;
        }
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            self.quit = true;
            return;
        }
        if self.show_help {
            self.show_help = false;
            return;
        }
        match key.code {
            KeyCode::Tab => return self.cycle_focus(true),
            KeyCode::BackTab => return self.cycle_focus(false),
            KeyCode::PageUp => return self.scroll_by(self.view_height as isize / 2),
            KeyCode::PageDown => return self.scroll_by(-(self.view_height as isize / 2)),
            _ => {}
        }
        match self.focus {
            Focus::Input => self.on_input_key(key),
            Focus::Agents | Focus::Logs => self.on_list_key(key),
        }
    }

    fn cycle_focus(&mut self, forward: bool) {
        let order = [Focus::Agents, Focus::Logs, Focus::Input];
        let i = order.iter().position(|f| *f == self.focus).unwrap_or(0);
        let n = order.len();
        self.focus = order[if forward {
            (i + 1) % n
        } else {
            (i + n - 1) % n
        }];
    }

    fn scroll_by(&mut self, delta: isize) {
        self.scroll = (self.scroll as isize + delta).max(0) as usize;
    }

    fn on_list_key(&mut self, key: KeyEvent) {
        let (sel, len) = match self.focus {
            Focus::Agents => (&mut self.agent_sel, self.snap.agents.len()),
            _ => (&mut self.log_sel, self.snap.logs.len()),
        };
        match key.code {
            KeyCode::Up | KeyCode::Char('k') => *sel = sel.saturating_sub(1),
            KeyCode::Down | KeyCode::Char('j') => *sel = (*sel + 1).min(len.saturating_sub(1)),
            KeyCode::Enter | KeyCode::Right | KeyCode::Char('l') => {
                if let Some(kind) = self.selected_kind() {
                    self.open(kind);
                    self.focus = Focus::Input;
                }
            }
            KeyCode::Char('q') => self.quit = true,
            KeyCode::Char('?') => self.show_help = true,
            KeyCode::Char('i') | KeyCode::Char('/') | KeyCode::Char(':') => {
                self.focus = Focus::Input;
                if key.code == KeyCode::Char('/') {
                    self.set_input("/");
                }
            }
            KeyCode::Char('s') => {
                self.focus = Focus::Input;
                self.set_input("/spawn ");
            }
            KeyCode::Char('t') => {
                self.focus = Focus::Input;
                self.set_input("/topic ");
            }
            KeyCode::Char('x') if self.focus == Focus::Agents => {
                if let Some(ViewKind::Agent(id)) = self.selected_kind() {
                    self.focus = Focus::Input;
                    self.set_input(&format!("/kill {id}"));
                }
            }
            KeyCode::Char('G') | KeyCode::End => self.scroll = 0,
            KeyCode::Home => self.scroll = usize::MAX / 2,
            _ => {}
        }
    }

    fn selected_kind(&self) -> Option<ViewKind> {
        match self.focus {
            Focus::Agents => self
                .agent_rows()
                .get(self.agent_sel)
                .map(|(_, a)| ViewKind::Agent(a.id.clone())),
            Focus::Logs => self
                .log_rows()
                .get(self.log_sel)
                .map(|l| ViewKind::Log(l.log.clone())),
            Focus::Input => None,
        }
    }

    fn set_input(&mut self, s: &str) {
        self.input = s.to_string();
        self.cursor = s.chars().count();
    }

    fn on_input_key(&mut self, key: KeyEvent) {
        let byte = |s: &str, c: usize| s.char_indices().nth(c).map_or(s.len(), |(i, _)| i);
        match key.code {
            KeyCode::Esc => self.focus = Focus::Agents,
            KeyCode::Enter => {
                let line = std::mem::take(&mut self.input);
                self.cursor = 0;
                self.submit(line.trim());
            }
            KeyCode::Char(c) => {
                let at = byte(&self.input, self.cursor);
                self.input.insert(at, c);
                self.cursor += 1;
            }
            KeyCode::Backspace if self.cursor > 0 => {
                self.cursor -= 1;
                let at = byte(&self.input, self.cursor);
                self.input.remove(at);
            }
            KeyCode::Delete if self.cursor < self.input.chars().count() => {
                let at = byte(&self.input, self.cursor);
                self.input.remove(at);
            }
            KeyCode::Left => self.cursor = self.cursor.saturating_sub(1),
            KeyCode::Right => self.cursor = (self.cursor + 1).min(self.input.chars().count()),
            KeyCode::Home => self.cursor = 0,
            KeyCode::End => self.cursor = self.input.chars().count(),
            KeyCode::Up => self.scroll_by(1),
            KeyCode::Down => self.scroll_by(-1),
            _ => {}
        }
    }

    pub fn on_app_event(&mut self, ev: AppEvent) {
        match ev {
            AppEvent::Snapshot(snap) => {
                self.fetching = false;
                let keep_agent = self
                    .agent_rows()
                    .get(self.agent_sel)
                    .map(|(_, a)| a.id.clone());
                let keep_log = self.log_rows().get(self.log_sel).map(|l| l.log.clone());
                self.snap = snap;
                if let Some(id) = keep_agent {
                    self.agent_sel = self
                        .agent_rows()
                        .iter()
                        .position(|(_, a)| a.id == id)
                        .unwrap_or(0);
                }
                if let Some(key) = keep_log {
                    self.log_sel = self
                        .log_rows()
                        .iter()
                        .position(|l| l.log == key)
                        .unwrap_or(0);
                }
            }
            AppEvent::Info(s) => self.status = (s, false),
            AppEvent::Error(s) => {
                self.fetching = false;
                self.status = (s, true);
            }
            AppEvent::Open(kind) => self.open(kind),
        }
    }

    pub fn on_tick(&mut self) {
        if self.state_dirty && !self.fetching {
            self.state_dirty = false;
            self.fetching = true;
            let api = self.api.clone();
            let tx = self.ev_tx.clone();
            tokio::spawn(async move {
                let _ = tx.send(match api.state().await {
                    Ok(s) => AppEvent::Snapshot(s),
                    Err(e) => AppEvent::Error(format!("{e:#}")),
                });
            });
        }
    }

    pub fn on_frame(&mut self, tag: Tag, frame: WsFrame) {
        match tag {
            Tag::Feed => {
                if let WsFrame::Feed { event } = frame {
                    self.on_feed(event);
                } else if let WsFrame::Synced = frame {
                    self.state_dirty = true;
                }
            }
            Tag::View(g) => {
                let Some(view) = self.view.as_mut().filter(|v| v.generation == g) else {
                    return;
                };
                match frame {
                    WsFrame::Flow { item, .. } => match item {
                        FlowItem::Delta {
                            thinking: true,
                            delta,
                        } => view.stream_thinking.push_str(&delta),
                        FlowItem::Delta {
                            thinking: false,
                            delta,
                        } => view.stream_text.push_str(&delta),
                        item => {
                            if matches!(item, FlowItem::Assistant { .. } | FlowItem::Status { .. })
                            {
                                view.stream_text.clear();
                                view.stream_thinking.clear();
                            }
                            view.push(Entry::Flow(item));
                        }
                    },
                    WsFrame::Message { who, msg, .. } => view.push(Entry::Msg { who, msg }),
                    WsFrame::Synced => view.synced = true,
                    WsFrame::Error { error } => view.error = Some(error),
                    WsFrame::Feed { .. } => {}
                }
            }
        }
    }

    fn on_feed(&mut self, ev: Feed) {
        let time = chrono::Local::now().format("%H:%M:%S").to_string();
        let mut line = |color: Color, text: String| {
            self.activity.push_back((time.clone(), color, text));
            if self.activity.len() > MAX_ACTIVITY {
                self.activity.pop_front();
            }
        };
        match &ev {
            Feed::Message {
                msg, title, who, ..
            } => {
                let first = msg.text.lines().next().unwrap_or("");
                let whom = short(who);
                let target = match short_title(title).split_once(" ↔ ") {
                    Some((a, b)) => format!("✉ {}", if a == whom { b } else { a }),
                    None => format!("→ {}", short_title(title)),
                };
                line(Color::Cyan, format!("{whom} {target}: {first}"));
                let viewing = matches!(&self.view, Some(View { kind: ViewKind::Log(l), .. }) if *l == msg.log);
                if !viewing && msg.from != HUMAN {
                    *self.unread.entry(msg.log.clone()).or_default() += 1;
                    if is_human_dm(&msg.log) || msg.mentions.iter().any(|m| m == HUMAN) {
                        self.status = (format!("✉ message for @human from {}", short(who)), false);
                    }
                }
            }
            Feed::AgentCreated { who, parent, .. } => line(
                Color::Green,
                format!("+ {} (by {})", short(who), short(parent)),
            ),
            Feed::AgentStatus { who, status, .. } => line(
                Color::DarkGray,
                format!("{} {}", short(who), format!("{status:?}").to_lowercase()),
            ),
            Feed::ToolStart { who, tool, .. } => {
                line(Color::Yellow, format!("{} ⚙ {tool}", short(who)))
            }
            Feed::ToolEnd {
                who,
                tool,
                is_error: true,
                ..
            } => line(Color::Red, format!("{} ✗ {tool}", short(who))),
            Feed::NameClaimed { id, name } => line(Color::Green, format!("{id} is @{name}")),
            Feed::TopicCreated { topic, by, .. } => line(
                Color::Green,
                format!("+ {} (by {})", short_title(topic), short(by)),
            ),
            Feed::TopicNamed { id, name, .. } => line(Color::Green, format!("{id} is #{name}")),
            Feed::Subscribed { who, topic, .. } => line(
                Color::DarkGray,
                format!("{} ⊕ {}", short(who), short_title(topic)),
            ),
            Feed::Unsubscribed { who, topic } => line(
                Color::DarkGray,
                format!("{} ⊖ {}", short(who), short_title(topic)),
            ),
            Feed::Error { who, error } => line(Color::Red, format!("{}: {error}", short(who))),
            Feed::ToolEnd { .. } | Feed::AssistantText { .. } => {}
        }
        if !matches!(
            ev,
            Feed::ToolStart { .. } | Feed::ToolEnd { .. } | Feed::AssistantText { .. }
        ) {
            self.state_dirty = true;
        }
    }

    // ------------------------------------------------------------ actions

    pub fn open(&mut self, kind: ViewKind) {
        if let Some(task) = self.view_task.take() {
            task.abort();
        }
        self.next_gen += 1;
        let generation = self.next_gen;
        if let ViewKind::Log(log) = &kind {
            self.unread.remove(log);
        }
        let (key, value) = match &kind {
            ViewKind::Agent(id) => ("agent", id.clone()),
            ViewKind::Log(log) => ("log", log.clone()),
        };
        self.view = Some(View {
            kind,
            generation,
            entries: VecDeque::new(),
            stream_text: String::new(),
            stream_thinking: String::new(),
            synced: false,
            error: None,
        });
        self.scroll = 0;
        let api = self.api.clone();
        let tx = self.ws_tx.clone();
        let ev_tx = self.ev_tx.clone();
        self.view_task = Some(tokio::spawn(async move {
            if let Err(e) = api
                .stream(&[(key, &value)], Tag::View(generation), tx)
                .await
            {
                let _ = ev_tx.send(AppEvent::Error(format!("view stream: {e:#}")));
            }
        }));
    }

    fn submit(&mut self, line: &str) {
        if line.is_empty() {
            return;
        }
        if let Some(cmd) = line.strip_prefix('/') {
            return self.command(cmd);
        }
        let (head, rest) = line
            .split_once(char::is_whitespace)
            .map_or((line, ""), |(h, r)| (h, r.trim()));
        if head.starts_with('@') && !rest.is_empty() {
            return self.request("send", json!({ "to": head, "text": rest }), None);
        }
        if head.starts_with('#') && head.len() > 1 && !rest.is_empty() {
            return self.request("post", json!({ "topic": head, "text": rest }), None);
        }
        match self.view.as_ref().map(|v| v.kind.clone()) {
            Some(ViewKind::Agent(id)) => {
                self.request("send", json!({ "to": id, "text": line }), None)
            }
            Some(ViewKind::Log(log)) if !log.starts_with("dm:") => {
                self.request("post", json!({ "topic": log, "text": line }), None)
            }
            Some(ViewKind::Log(log)) => match dm_other(&log) {
                Some(other) => self.request("send", json!({ "to": other, "text": line }), None),
                None => self.fail("this DM is between two agents; message one of them with @name"),
            },
            None => self.fail("open an agent or topic first, or start with @agent / #topic"),
        }
    }

    fn command(&mut self, cmd: &str) {
        let mut words = cmd.split_whitespace();
        let name = words.next().unwrap_or("");
        let rest: Vec<&str> = words.collect();
        match name {
            "q" | "quit" => self.quit = true,
            "help" | "h" => self.show_help = true,
            "spawn" => {
                let mut body = json!({});
                let mut prompt = Vec::new();
                let mut it = rest.iter();
                while let Some(w) = it.next() {
                    match *w {
                        "--name" | "--model" | "--thinking" | "--cwd" | "--tools" => {
                            match it.next() {
                                Some(v) => body[&w[2..]] = json!(v),
                                None => return self.fail(&format!("{w} needs a value")),
                            }
                        }
                        _ => prompt.push(*w),
                    }
                }
                if prompt.is_empty() {
                    return self.fail(
                        "usage: /spawn [--name N] [--model M] [--thinking L] [--cwd D] <prompt>",
                    );
                }
                body["prompt"] = json!(prompt.join(" "));
                self.request(
                    "spawn",
                    body,
                    Some(|data| data["id"].as_str().map(|id| ViewKind::Agent(id.into()))),
                );
            }
            "topic" => {
                let Some((topic, desc)) = rest.split_first() else {
                    return self.fail("usage: /topic <name> [description]");
                };
                let body = json!({ "name": topic.trim_start_matches('#'), "description": (!desc.is_empty()).then(|| desc.join(" ")) });
                self.request(
                    "topic",
                    body,
                    Some(|data| data["id"].as_str().map(|id| ViewKind::Log(id.into()))),
                );
            }
            "invite" => match self.view.as_ref().map(|v| v.kind.clone()) {
                Some(ViewKind::Log(log)) if !rest.is_empty() && !log.starts_with("dm:") => self
                    .request(
                        "topic/invite",
                        json!({ "topic": log, "agents": rest }),
                        None,
                    ),
                _ => self.fail("usage (in a topic view): /invite @agent [@agent...]"),
            },
            "rename" => match (self.view.as_ref().map(|v| v.kind.clone()), rest.first()) {
                (Some(ViewKind::Log(log)), Some(new)) if !log.starts_with("dm:") => self.request(
                    "topic/rename",
                    json!({ "topic": log, "name": new.trim_start_matches('#') }),
                    None,
                ),
                _ => self.fail("usage (in a topic view): /rename <name>"),
            },
            "kill" => match rest.first() {
                Some(agent) => self.request("kill", json!({ "agent": agent }), None),
                None => self.fail("usage: /kill <agent>"),
            },
            "open" => match rest.first() {
                Some(t) if t.starts_with('@') || t.starts_with("a-") => {
                    let spec = t.trim_start_matches('@');
                    match self
                        .snap
                        .agents
                        .iter()
                        .find(|a| a.id == spec || a.name.as_deref() == Some(spec))
                    {
                        Some(a) => self.open(ViewKind::Agent(a.id.clone())),
                        None => self.fail(&format!("no agent {t}")),
                    }
                }
                Some(t) => {
                    let spec = t.trim_start_matches('#');
                    match self
                        .snap
                        .logs
                        .iter()
                        .find(|l| l.log == spec || l.name.as_deref() == Some(spec))
                    {
                        Some(l) => self.open(ViewKind::Log(l.log.clone())),
                        None => self.fail(&format!("no log {t}")),
                    }
                }
                None => self.fail("usage: /open @agent | #topic | dm:..."),
            },
            "dm" => match rest.first() {
                Some(agent) => {
                    let spec = agent.trim_start_matches('@');
                    match self
                        .snap
                        .agents
                        .iter()
                        .find(|a| a.id == spec || a.name.as_deref() == Some(spec))
                    {
                        Some(a) => {
                            let (x, y) = if a.id.as_str() <= HUMAN {
                                (a.id.as_str(), HUMAN)
                            } else {
                                (HUMAN, a.id.as_str())
                            };
                            self.open(ViewKind::Log(format!("dm:{x}+{y}")));
                        }
                        None => self.fail(&format!("no agent {agent}")),
                    }
                }
                None => self.fail("usage: /dm @agent"),
            },
            _ => self.fail(&format!("unknown command /{name} (see /help)")),
        }
    }

    fn fail(&mut self, msg: &str) {
        self.status = (msg.to_string(), true);
    }

    /// Fire an API call in the background; on success show its text and optionally open a view.
    fn request(
        &mut self,
        path: &'static str,
        body: serde_json::Value,
        then: Option<fn(&serde_json::Value) -> Option<ViewKind>>,
    ) {
        let api = self.api.clone();
        let tx = self.ev_tx.clone();
        tokio::spawn(async move {
            match api.call(path, body).await {
                Ok(resp) => {
                    let data = resp.data.unwrap_or_default();
                    let _ = tx.send(AppEvent::Info(resp.text.unwrap_or_default()));
                    if let Some(kind) = then.and_then(|f| f(&data)) {
                        let _ = tx.send(AppEvent::Open(kind));
                    }
                }
                Err(e) => {
                    let _ = tx.send(AppEvent::Error(format!("{e:#}")));
                }
            }
        });
    }
}

impl View {
    fn push(&mut self, e: Entry) {
        self.entries.push_back(e);
        if self.entries.len() > MAX_ENTRIES {
            self.entries.pop_front();
        }
    }
}

pub fn is_human_dm(log: &str) -> bool {
    log.strip_prefix("dm:")
        .and_then(|p| p.split_once('+'))
        .is_some_and(|(a, b)| a == HUMAN || b == HUMAN)
}

/// The non-human participant of a DM with the human.
fn dm_other(log: &str) -> Option<String> {
    let (a, b) = log.strip_prefix("dm:")?.split_once('+')?;
    match (a == HUMAN, b == HUMAN) {
        (true, _) => Some(b.to_string()),
        (_, true) => Some(a.to_string()),
        _ => None,
    }
}

/// `@name (a-123)` → `@name`.
pub fn short(label: &str) -> String {
    match label.split_once(" (") {
        Some((name, _)) if name.starts_with('@') => name.to_string(),
        _ => label.to_string(),
    }
}

/// `#plan (t-123)` → `#plan`; DM titles shortened on both sides.
pub fn short_title(title: &str) -> String {
    if let Some(rest) = title.strip_prefix("DM ")
        && let Some((a, b)) = rest.split_once(" ↔ ")
    {
        return format!("{} ↔ {}", short(a), short(b));
    }
    match title.split_once(" (") {
        Some((name, _)) if name.starts_with('#') => name.to_string(),
        _ => title.to_string(),
    }
}
