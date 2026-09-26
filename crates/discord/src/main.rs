//! agentcord-discord: a standalone bridge between one Discord channel and one agentcord topic.
//!
//! - Every post in the topic is mirrored into the channel through a webhook, under the poster's
//!   name (`alice`, `bob`, `human`, ...).
//! - Every message a human writes in the channel is posted to the topic as `@human`, which wakes
//!   the topic's subscribers (and anyone @-mentioned).
//!
//! It talks to the agentcord daemon only through its HTTP/WebSocket API. Progress in both
//! directions (last mirrored topic seq, last forwarded Discord message id) is kept in a state file,
//! so a restarted bridge catches up without duplicates.

mod daemon;

use agentcord_proto::{HUMAN, Message as TopicMessage, MsgKind, WsFrame};
use anyhow::{Context as _, Result, bail};
use clap::Parser;
use daemon::Daemon;
use serde::{Deserialize, Serialize};
use serenity::all::{
    ChannelId, Context, CreateAllowedMentions, CreateWebhook, EditChannel, EventHandler,
    ExecuteWebhook, GatewayIntents, GetMessages, Http, Message, MessageId, ReactionType, Ready,
    Webhook,
};
use serenity::async_trait;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

const WEBHOOK_NAME: &str = "agentcord";
/// Discord's limit is 2000 characters per message; leave room for a kind tag.
const CHUNK: usize = 1900;

/// Mirror an agentcord topic into a Discord channel, and post the channel's human messages to the
/// topic as @human.
#[derive(Parser)]
#[command(version)]
struct Cli {
    /// Discord bot token. The bot needs the Message Content intent, and Manage Webhooks in the
    /// channel (unless --webhook-url is given).
    #[arg(long, env = "DISCORD_TOKEN", hide_env_values = true)]
    discord_token: String,
    /// The channel to take over.
    #[arg(long, env = "DISCORD_CHANNEL_ID")]
    channel: u64,
    /// The topic to mirror (`#name` or `t-...`). Created if it doesn't exist.
    #[arg(long, env = "AGENTCORD_TOPIC")]
    topic: String,
    /// Use this webhook instead of creating/reusing one named "agentcord" in the channel.
    #[arg(long, env = "DISCORD_WEBHOOK_URL", hide_env_values = true)]
    webhook_url: Option<String>,
    /// agentcord session directory (its api.json locates the daemon). Defaults to the newest
    /// under ./.agentcord/sessions.
    #[arg(long, env = "AGENTCORD_SESSION")]
    session: Option<PathBuf>,
    /// Daemon API URL, instead of reading <session>/api.json.
    #[arg(long, requires = "token")]
    url: Option<String>,
    /// Daemon API token.
    #[arg(long, env = "AGENTCORD_TOKEN", hide_env_values = true)]
    token: Option<String>,
    /// Where to keep bridge progress. Default: ./agentcord-discord-<channel>.json
    #[arg(long)]
    state: Option<PathBuf>,
    /// On first run, also mirror this many existing topic messages.
    #[arg(long, default_value_t = 0)]
    replay: usize,
    /// Prefix forwarded messages with the Discord author's name, e.g. "[alice on Discord] ...".
    #[arg(long)]
    attribute: bool,
    /// Avatar URL template for mirrored posters; `{name}` is replaced by the poster's name.
    /// Note that this sends agent names to whatever service the URL points at.
    #[arg(long)]
    avatar_template: Option<String>,
    /// Don't rewrite the channel's topic line.
    #[arg(long)]
    keep_channel_topic: bool,
}

#[derive(Serialize, Deserialize, Default)]
struct State {
    /// Topic id this state belongs to; a different topic starts fresh.
    topic: String,
    /// Last topic seq mirrored to Discord.
    last_seq: Option<u64>,
    /// Last Discord message forwarded to the topic.
    last_discord_id: Option<u64>,
}

struct Bridge {
    daemon: Daemon,
    http: Arc<Http>,
    channel: ChannelId,
    webhook: Webhook,
    topic: String,
    topic_title: String,
    attribute: bool,
    avatar_template: Option<String>,
    replay: usize,
    state_path: PathBuf,
    state: Mutex<State>,
    /// Serializes Discord → topic forwarding (live messages vs. startup catch-up).
    forwarding: Mutex<()>,
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("agentcord-discord: {e:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();
    let daemon = Daemon::connect(cli.url.clone(), cli.token.clone(), cli.session.clone())?;
    let (topic, topic_title) = daemon.ensure_topic(&cli.topic).await?;
    log(&format!("topic {topic_title} ↔ channel {}", cli.channel));

    let http = Arc::new(Http::new(&cli.discord_token));
    let channel = ChannelId::new(cli.channel);
    let webhook = webhook(&http, channel, cli.webhook_url.as_deref()).await?;

    let state_path = cli
        .state
        .clone()
        .unwrap_or_else(|| PathBuf::from(format!("agentcord-discord-{}.json", cli.channel)));
    let mut state: State = std::fs::read(&state_path)
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_default();
    let first_run = state.topic != topic;
    if first_run {
        state = State {
            topic: topic.clone(),
            ..Default::default()
        };
    }

    let bridge = Arc::new(Bridge {
        daemon,
        http: http.clone(),
        channel,
        webhook,
        topic,
        topic_title,
        attribute: cli.attribute,
        avatar_template: cli.avatar_template.clone(),
        replay: cli.replay,
        state_path,
        state: Mutex::new(state),
        forwarding: Mutex::new(()),
    });
    bridge.save().await;

    if !cli.keep_channel_topic {
        let line = format!(
            "Mirrors agentcord topic {}. Messages here are posted to it as @human; @name mentions wake agents.",
            bridge.topic_title
        );
        if let Err(e) = channel
            .edit(http.as_ref(), EditChannel::new().topic(line))
            .await
        {
            log(&format!(
                "could not set the channel topic (needs Manage Channels): {e}"
            ));
        }
    }
    if first_run {
        let text = format!(
            "Mirroring agentcord topic **{}** here. Anything you write in this channel is posted to the topic as **@human**.",
            bridge.topic_title
        );
        bridge.execute("agentcord", &text).await;
    }

    tokio::spawn(bridge.clone().mirror_topic());

    let intents = GatewayIntents::GUILD_MESSAGES | GatewayIntents::MESSAGE_CONTENT;
    let mut client = serenity::Client::builder(&cli.discord_token, intents)
        .event_handler(Handler { bridge })
        .await
        .context("building the Discord client")?;
    tokio::select! {
        r = client.start() => r.context("Discord gateway")?,
        _ = tokio::signal::ctrl_c() => log("interrupted"),
    }
    Ok(())
}

/// The webhook to post through: the given URL, or one named "agentcord" in the channel
/// (reused if this bot created it before, created otherwise).
async fn webhook(http: &Http, channel: ChannelId, url: Option<&str>) -> Result<Webhook> {
    if let Some(url) = url {
        return Webhook::from_url(http, url)
            .await
            .context("loading --webhook-url");
    }
    let existing = channel.webhooks(http).await.context(
        "listing channel webhooks (the bot needs Manage Webhooks, or pass --webhook-url)",
    )?;
    if let Some(hook) = existing
        .into_iter()
        .find(|w| w.name.as_deref() == Some(WEBHOOK_NAME) && w.token.is_some())
    {
        return Ok(hook);
    }
    channel
        .create_webhook(http, CreateWebhook::new(WEBHOOK_NAME))
        .await
        .context("creating the channel webhook (the bot needs Manage Webhooks)")
}

impl Bridge {
    async fn save(&self) {
        let state = self.state.lock().await;
        let bytes = serde_json::to_vec_pretty(&*state).unwrap_or_default();
        let tmp = self.state_path.with_extension("tmp");
        if let Err(e) =
            std::fs::write(&tmp, bytes).and_then(|_| std::fs::rename(&tmp, &self.state_path))
        {
            log(&format!("saving state: {e}"));
        }
    }

    // ------------------------------------------------------------ topic → Discord

    /// Follow the topic's WebSocket stream forever, reconnecting as needed.
    async fn mirror_topic(self: Arc<Self>) {
        loop {
            if let Err(e) = self.mirror_once().await {
                log(&format!("topic stream: {e:#}; reconnecting in 3s"));
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }

    async fn mirror_once(&self) -> Result<()> {
        let mut stream = self.daemon.log_stream(&self.topic, 1000).await?;
        let mut backlog = Vec::new();
        while let Some(frame) = stream.next_frame().await? {
            match frame {
                WsFrame::Message {
                    live: false,
                    who,
                    msg,
                } => backlog.push((who, msg)),
                WsFrame::Synced => {
                    let last = self.state.lock().await.last_seq;
                    let start = match last {
                        Some(seq) => backlog
                            .iter()
                            .position(|(_, m)| m.seq > seq)
                            .unwrap_or(backlog.len()),
                        None => backlog.len().saturating_sub(self.replay),
                    };
                    if last.is_none() {
                        // First run: everything before the replay window counts as seen.
                        let seen = start.checked_sub(1).map(|i| backlog[i].1.seq).unwrap_or(0);
                        self.state.lock().await.last_seq = Some(seen);
                        self.save().await;
                    }
                    for (who, msg) in backlog.drain(..).skip(start) {
                        self.mirror(&who, &msg).await;
                    }
                    log("topic stream synced");
                }
                WsFrame::Message {
                    live: true,
                    who,
                    msg,
                } => self.mirror(&who, &msg).await,
                WsFrame::Error { error } => bail!("{error}"),
                _ => {}
            }
        }
        bail!("stream closed")
    }

    async fn mirror(&self, who: &str, msg: &TopicMessage) {
        if self
            .state
            .lock()
            .await
            .last_seq
            .is_some_and(|s| msg.seq <= s)
        {
            return;
        }
        // Human posts (from this channel, the TUI or the CLI) are never sent out the webhook.
        if msg.from != HUMAN {
            let tag = match msg.kind {
                MsgKind::Message => String::new(),
                MsgKind::Task => "*[task]* ".into(),
                MsgKind::Report => "*[report]* ".into(),
                MsgKind::System => "*[notice]* ".into(),
            };
            self.execute(&poster_name(who), &format!("{tag}{}", msg.text))
                .await;
        }
        self.state.lock().await.last_seq = Some(msg.seq);
        self.save().await;
    }

    /// Post through the webhook under `name`, split into Discord-sized chunks. Mentions are
    /// disabled so agents can't ping @everyone or users.
    async fn execute(&self, name: &str, text: &str) {
        for chunk in chunks(text, CHUNK) {
            let mut builder = ExecuteWebhook::new()
                .content(chunk)
                .username(name)
                .allowed_mentions(CreateAllowedMentions::new());
            if let Some(t) = &self.avatar_template {
                builder = builder.avatar_url(t.replace("{name}", name));
            }
            if let Err(e) = self
                .webhook
                .execute(self.http.as_ref(), true, builder)
                .await
            {
                log(&format!("webhook post as {name} failed: {e}"));
            }
        }
    }

    // ------------------------------------------------------------ Discord → topic

    /// Forward messages written while the bridge was down.
    async fn catch_up(&self) {
        let Some(after) = self.state.lock().await.last_discord_id else {
            return;
        };
        let mut cursor = MessageId::new(after);
        loop {
            let batch = match self
                .channel
                .messages(
                    self.http.as_ref(),
                    GetMessages::new().after(cursor).limit(100),
                )
                .await
            {
                Ok(b) => b,
                Err(e) => return log(&format!("catch-up failed: {e}")),
            };
            if batch.is_empty() {
                return;
            }
            let mut batch = batch;
            batch.sort_by_key(|m| m.id);
            cursor = batch.last().unwrap().id;
            let n = batch.len();
            for m in batch {
                self.forward(&m).await;
            }
            log(&format!("caught up {n} Discord message(s)"));
            if n < 100 {
                return;
            }
        }
    }

    async fn forward(&self, m: &Message) {
        if m.channel_id != self.channel || m.webhook_id.is_some() || m.author.bot {
            return;
        }
        let _serial = self.forwarding.lock().await;
        if self
            .state
            .lock()
            .await
            .last_discord_id
            .is_some_and(|id| m.id.get() <= id)
        {
            return;
        }
        let mut text = m.content.clone();
        for a in &m.attachments {
            text.push_str(&format!("\n[attachment: {} {}]", a.filename, a.url));
        }
        if !text.trim().is_empty() {
            if self.attribute {
                let who = m.author.global_name.as_deref().unwrap_or(&m.author.name);
                text = format!("[{who} on Discord] {text}");
            }
            if let Err(e) = self.daemon.post(&self.topic, &text).await {
                log(&format!("posting to the topic failed: {e:#}"));
                let _ = m
                    .react(self.http.as_ref(), ReactionType::Unicode("⚠️".into()))
                    .await;
            }
        }
        self.state.lock().await.last_discord_id = Some(m.id.get());
        self.save().await;
    }
}

struct Handler {
    bridge: Arc<Bridge>,
}

#[async_trait]
impl EventHandler for Handler {
    async fn ready(&self, _ctx: Context, ready: Ready) {
        log(&format!("connected to Discord as {}", ready.user.name));
        self.bridge.catch_up().await;
    }

    async fn message(&self, _ctx: Context, msg: Message) {
        self.bridge.forward(&msg).await;
    }
}

/// `@alice (a-123)` → `alice`; `a-123` stays; `@human` → `human`. Discord rejects webhook names
/// containing "discord" or "clyde", and caps them at 80 characters.
fn poster_name(label: &str) -> String {
    let name = label
        .split(" (")
        .next()
        .unwrap_or(label)
        .trim_start_matches('@');
    let name = name.replace("discord", "d1scord").replace("clyde", "c1yde");
    let name: String = name.chars().take(80).collect();
    if name.is_empty() {
        "agent".into()
    } else {
        name
    }
}

/// Split at line boundaries (or hard-split long lines) into pieces of at most `max` chars.
fn chunks(text: &str, max: usize) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    for line in text.split_inclusive('\n') {
        if cur.chars().count() + line.chars().count() > max && !cur.is_empty() {
            out.push(std::mem::take(&mut cur));
        }
        let mut line = line;
        while line.chars().count() > max {
            let cut = line.char_indices().nth(max).map_or(line.len(), |(i, _)| i);
            out.push(line[..cut].to_string());
            line = &line[cut..];
        }
        cur.push_str(line);
    }
    if !cur.trim().is_empty() || out.is_empty() {
        out.push(cur);
    }
    out
}

fn log(msg: &str) {
    eprintln!("agentcord-discord: {msg}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names() {
        assert_eq!(poster_name("@alice (a-1234)"), "alice");
        assert_eq!(poster_name("a-1234"), "a-1234");
        assert_eq!(poster_name("@human"), "human");
        assert_eq!(poster_name("@discordian (a-1)"), "d1scordian");
    }

    #[test]
    fn chunking() {
        assert_eq!(chunks("short", 10), vec!["short"]);
        let text = "aaaa\nbbbb\ncccc\n";
        assert_eq!(chunks(text, 10), vec!["aaaa\nbbbb\n", "cccc\n"]);
        let long = "x".repeat(25);
        assert_eq!(
            chunks(&long, 10),
            vec!["x".repeat(10), "x".repeat(10), "x".repeat(5)]
        );
        assert!(
            chunks(&"é".repeat(3000), 1900)
                .iter()
                .all(|c| c.chars().count() <= 1900)
        );
    }
}
