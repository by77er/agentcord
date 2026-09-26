//! agentcord-tui: watch and steer an agentcord swarm as @human.

mod app;
mod net;
mod ui;

use anyhow::{Context, Result, bail};
use app::{App, AppEvent, Tag};
use clap::Parser;
use futures_util::StreamExt;
use net::Api;
use ratatui::crossterm::event::EventStream;
use serde_json::json;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::sync::mpsc;

const SESSIONS: &str = ".agentcord/sessions";

/// Terminal UI for agentcord: agents, topics and DMs, live agent flows, and messaging as @human.
#[derive(Parser)]
#[command(version)]
struct Cli {
    /// Session directory (its api.json locates the daemon). Defaults to the newest under
    /// ./.agentcord/sessions, or a new one with --start.
    #[arg(long, env = "AGENTCORD_SESSION")]
    session: Option<PathBuf>,
    /// Daemon API URL, instead of reading <session>/api.json.
    #[arg(long, requires = "token")]
    url: Option<String>,
    #[arg(long, env = "AGENTCORD_TOKEN")]
    token: Option<String>,
    /// Start a daemon (`agentcord serve`) for the session if none is running. It keeps running
    /// after the TUI exits; stop it with `agentcord shutdown`.
    #[arg(long)]
    start: bool,
    /// agentcord executable used by --start (default: next to this binary, else PATH).
    #[arg(long)]
    agentcord_bin: Option<PathBuf>,
    /// With --start: default model for agents.
    #[arg(long)]
    model: Option<String>,
    /// With --start: default thinking level.
    #[arg(long)]
    thinking: Option<String>,
    /// With --start: run agents without pi's default tools (only the agentcord tools).
    #[arg(long)]
    no_builtin_tools: bool,
    /// With --start: load your global pi extensions into agents.
    #[arg(long)]
    user_extensions: bool,
    /// With --start: extra pi argument (repeatable).
    #[arg(long = "pi-arg", allow_hyphen_values = true)]
    pi_args: Vec<String>,
    /// Name for the root agent spawned from PROMPT.
    #[arg(long)]
    name: Option<String>,
    /// Spawn a root agent with this task on startup (starts the swarm).
    prompt: Vec<String>,
}

#[tokio::main]
async fn main() {
    if let Err(e) = real_main().await {
        eprintln!("agentcord-tui: {e:#}");
        std::process::exit(1);
    }
}

async fn real_main() -> Result<()> {
    let cli = Cli::parse();
    let api = connect(&cli).await?;
    api.state()
        .await
        .context("cannot reach the daemon (use --start to launch one)")?;

    let (ws_tx, mut ws_rx) = mpsc::unbounded_channel();
    let (ev_tx, mut ev_rx) = mpsc::unbounded_channel();
    let mut app = App::new(api.clone(), ws_tx.clone(), ev_tx.clone());

    // Global feed, reconnecting if the daemon restarts.
    {
        let api = api.clone();
        let ev_tx = ev_tx.clone();
        tokio::spawn(async move {
            loop {
                let result = api.stream(&[], Tag::Feed, ws_tx.clone()).await;
                let why = result
                    .err()
                    .map_or("closed".to_string(), |e| format!("{e:#}"));
                let _ = ev_tx.send(AppEvent::Error(format!(
                    "feed disconnected ({why}); retrying"
                )));
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
    }

    if !cli.prompt.is_empty() {
        let mut body = json!({ "prompt": cli.prompt.join(" ") });
        if let Some(name) = &cli.name {
            body["name"] = json!(name);
        }
        let resp = api.call("spawn", body).await?;
        if let Some(id) = resp.data.as_ref().and_then(|d| d["id"].as_str()) {
            app.open(app::ViewKind::Agent(id.to_string()));
            app.focus = app::Focus::Input;
        }
    }

    let mut terminal = ratatui::init();
    let mut events = EventStream::new();
    let mut tick = tokio::time::interval(Duration::from_millis(200));
    let result: Result<()> = async {
        loop {
            terminal.draw(|f| ui::draw(f, &mut app))?;
            tokio::select! {
                Some(ev) = events.next() => app.on_term(ev?),
                Some((tag, frame)) = ws_rx.recv() => {
                    app.on_frame(tag, frame);
                    // Coalesce bursts (streaming deltas) into one redraw.
                    while let Ok((tag, frame)) = ws_rx.try_recv() {
                        app.on_frame(tag, frame);
                    }
                }
                Some(ev) = ev_rx.recv() => app.on_app_event(ev),
                _ = tick.tick() => app.on_tick(),
            }
            if app.quit {
                return Ok(());
            }
        }
    }
    .await;
    ratatui::restore();
    result
}

async fn connect(cli: &Cli) -> Result<Api> {
    if let (Some(url), Some(token)) = (&cli.url, &cli.token) {
        return Ok(Api::new(url.clone(), token.clone()));
    }
    let session = match (&cli.session, cli.start) {
        (Some(s), _) => s.clone(),
        (None, true) => {
            PathBuf::from(SESSIONS).join(chrono::Local::now().format("%Y%m%d-%H%M%S").to_string())
        }
        (None, false) => latest_session()?,
    };
    if let Some(api) = read_api(&session)
        && api.state().await.is_ok()
    {
        return Ok(api);
    }
    if !cli.start {
        bail!(
            "no daemon running for {} (run `agentcord serve` or pass --start)",
            session.display()
        );
    }
    start_daemon(cli, &session)?;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Some(api) = read_api(&session)
            && api.state().await.is_ok()
        {
            return Ok(api);
        }
    }
    bail!(
        "daemon did not come up; see {}",
        session.join("daemon.log").display()
    )
}

fn read_api(session: &Path) -> Option<Api> {
    let v: serde_json::Value =
        serde_json::from_slice(&std::fs::read(session.join("api.json")).ok()?).ok()?;
    Some(Api::new(
        v["url"].as_str()?.to_string(),
        v["token"].as_str()?.to_string(),
    ))
}

fn start_daemon(cli: &Cli, session: &Path) -> Result<()> {
    std::fs::create_dir_all(session)?;
    let bin = cli.agentcord_bin.clone().unwrap_or_else(|| {
        std::env::current_exe()
            .ok()
            .map(|p| p.with_file_name("agentcord"))
            .filter(|p| p.exists())
            .unwrap_or_else(|| PathBuf::from("agentcord"))
    });
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(session.join("daemon.log"))?;
    let mut cmd = std::process::Command::new(&bin);
    cmd.arg("serve").arg("--session").arg(session);
    if let Some(m) = &cli.model {
        cmd.arg("--model").arg(m);
    }
    if let Some(t) = &cli.thinking {
        cmd.arg("--thinking").arg(t);
    }
    if cli.no_builtin_tools {
        cmd.arg("--no-builtin-tools");
    }
    if cli.user_extensions {
        cmd.arg("--user-extensions");
    }
    for a in &cli.pi_args {
        cmd.arg(format!("--pi-arg={a}"));
    }
    // Own process group, so the TUI's Ctrl-C/exit doesn't take the swarm down.
    use std::os::unix::process::CommandExt;
    cmd.process_group(0)
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log)
        .spawn()
        .with_context(|| format!("starting {}", bin.display()))?;
    Ok(())
}

fn latest_session() -> Result<PathBuf> {
    let mut all: Vec<PathBuf> = std::fs::read_dir(SESSIONS)
        .with_context(|| {
            format!("no --session given and no {SESSIONS} directory here (try --start)")
        })?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.join("session.json").exists())
        .collect();
    all.sort();
    all.pop().context("no sessions found (try --start)")
}
