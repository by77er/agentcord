mod api;
mod broker;
mod flow;
mod server;
mod store;

use agentcord_proto::*;
use anyhow::{Context, Result, bail};
use broker::{Broker, Config};
use clap::{Args, Parser, Subcommand};
use serde::{Deserialize, Serialize};
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncBufReadExt;

/// A multi-agent harness: pi agents that spawn each other, DM each other by id or @name,
/// and talk in durable, subscribable pub/sub topics.
#[derive(Parser)]
#[command(version)]
struct Cli {
    /// Session directory. Defaults to a new one under ./.agentcord/sessions for serve/run,
    /// and to the most recent one there for other commands.
    #[arg(long, global = true, env = "AGENTCORD_SESSION")]
    session: Option<PathBuf>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the broker in the foreground.
    Serve(ServeOpts),
    /// Run the broker, spawn a root agent with PROMPT, and chat with it (stdin) while streaming activity.
    Run {
        #[command(flatten)]
        serve: ServeOpts,
        /// Name for the root agent.
        #[arg(long)]
        name: Option<String>,
        /// Exit once every agent is idle and nothing is queued.
        #[arg(long)]
        exit_when_idle: bool,
        /// Also show assistant text and tool results as they happen.
        #[arg(short, long)]
        verbose: bool,
        #[arg(required = true, num_args = 1..)]
        prompt: Vec<String>,
    },
    /// Spawn an agent (as @human).
    Spawn {
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        model: Option<String>,
        #[arg(long)]
        thinking: Option<String>,
        #[arg(long)]
        cwd: Option<String>,
        #[arg(required = true, num_args = 1..)]
        prompt: Vec<String>,
    },
    /// DM an agent (by id or @name) as @human.
    Send {
        to: String,
        #[arg(required = true, num_args = 1..)]
        text: Vec<String>,
    },
    /// Post to a topic as @human (an unknown #name is created).
    Post {
        topic: String,
        #[arg(required = true, num_args = 1..)]
        text: Vec<String>,
    },
    /// Create a topic.
    Topic {
        /// Name to claim for the topic.
        name: Option<String>,
        #[arg(long)]
        description: Option<String>,
        /// Agents to subscribe to it.
        #[arg(long, num_args = 1..)]
        invite: Vec<String>,
    },
    /// Rename a topic.
    RenameTopic { topic: String, name: String },
    /// List topics.
    Topics,
    /// List agents.
    Agents,
    /// Read a window of a topic (#name / id) or DM log (dm:... or @agent for your DMs).
    Read {
        log: String,
        #[arg(long)]
        before: Option<u64>,
        #[arg(long)]
        after: Option<u64>,
        #[arg(long, short = 'n')]
        limit: Option<usize>,
    },
    /// Kill an agent and its descendants.
    Kill { agent: String },
    /// Stream live activity.
    Watch {
        #[arg(short, long)]
        verbose: bool,
    },
    /// Stop the broker and all agents (they resume on the next `serve`).
    Shutdown,
}

#[derive(Args, Clone)]
struct ServeOpts {
    /// Default model for agents (provider/id), e.g. openai-codex/gpt-5.5.
    #[arg(long)]
    model: Option<String>,
    /// Default thinking level for agents.
    #[arg(long)]
    thinking: Option<String>,
    /// pi executable.
    #[arg(long)]
    pi_bin: Option<String>,
    /// Extra argument passed to every pi process (repeatable), e.g. --pi-arg=--no-extensions.
    #[arg(long = "pi-arg", allow_hyphen_values = true)]
    pi_args: Vec<String>,
    /// Working directory for root agents (default: current directory).
    #[arg(long)]
    cwd: Option<PathBuf>,
    /// Address for the HTTP/WebSocket API (port 0 picks a free port). URL and token are
    /// written to <session>/api.json.
    #[arg(long, default_value = "127.0.0.1:0")]
    http: std::net::SocketAddr,
    /// Don't start the HTTP/WebSocket API.
    #[arg(long)]
    no_http: bool,
    /// Run agents without pi's default tools (read, bash, edit, write, grep, find, ls), leaving only
    /// the agentcord tools. Saved with the session; `--no-builtin-tools=false` turns it back off.
    #[arg(long, num_args = 0..=1, default_missing_value = "true")]
    no_builtin_tools: Option<bool>,
    /// Load your globally installed pi extensions (~/.pi/agent/extensions, settings packages) into
    /// agents. Off by default. Saved with the session.
    #[arg(long, num_args = 0..=1, default_missing_value = "true")]
    user_extensions: Option<bool>,
    /// Debounce for waking idle agents: deliver once no new message has arrived for this many ms
    /// (default 1000; 0 delivers immediately). Saved with the session.
    #[arg(long)]
    debounce_ms: Option<u64>,
    /// Never hold the first queued message longer than this many ms (default 5000).
    #[arg(long)]
    debounce_max_ms: Option<u64>,
}

/// `session.json`: broker settings, so `serve --session DIR` resumes with the same configuration.
#[derive(Serialize, Deserialize, Default)]
struct SessionFile {
    created: String,
    pi_bin: String,
    pi_args: Vec<String>,
    model: Option<String>,
    thinking: Option<String>,
    cwd: PathBuf,
    #[serde(default)]
    no_builtin_tools: bool,
    #[serde(default)]
    user_extensions: bool,
    #[serde(default)]
    debounce_ms: Option<u64>,
    #[serde(default)]
    debounce_max_ms: Option<u64>,
}

const SESSIONS: &str = ".agentcord/sessions";

#[tokio::main]
async fn main() {
    if let Err(e) = real_main().await {
        eprintln!("agentcord: {e:#}");
        std::process::exit(1);
    }
}

async fn real_main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Serve(opts) => {
            let broker = start(cli.session, opts).await?;
            let printer = Printer::new(false);
            let mut rx = broker.feed.subscribe();
            tokio::spawn(async move {
                while let Ok(ev) = rx.recv().await {
                    if let Some(line) = printer.render(&ev) {
                        eprintln!("{line}");
                    }
                }
            });
            wait_for_exit(&broker).await;
            broker.stop().await;
            Ok(())
        }
        Cmd::Run {
            serve,
            name,
            exit_when_idle,
            verbose,
            prompt,
        } => {
            run(
                cli.session,
                serve,
                name,
                exit_when_idle,
                verbose,
                prompt.join(" "),
            )
            .await
        }
        cmd => client(cli.session, cmd).await,
    }
}

async fn start(session: Option<PathBuf>, opts: ServeOpts) -> Result<Arc<Broker>> {
    let session = match session {
        Some(s) => s,
        None => {
            let stamp = chrono::Local::now().format("%Y%m%d-%H%M%S");
            PathBuf::from(SESSIONS).join(stamp.to_string())
        }
    };
    std::fs::create_dir_all(&session)?;
    let session = session.canonicalize()?;
    let file = session.join("session.json");
    let saved: SessionFile = match std::fs::read(&file) {
        Ok(bytes) => serde_json::from_slice(&bytes).context("reading session.json")?,
        Err(_) => SessionFile {
            created: store::now(),
            ..Default::default()
        },
    };
    let resumed = file.exists();
    let cwd = match opts
        .cwd
        .or((!saved.cwd.as_os_str().is_empty()).then(|| saved.cwd.clone()))
    {
        Some(c) => c.canonicalize()?,
        None => std::env::current_dir()?,
    };
    let cfg = SessionFile {
        created: saved.created,
        pi_bin: opts.pi_bin.unwrap_or(if saved.pi_bin.is_empty() {
            "pi".into()
        } else {
            saved.pi_bin
        }),
        pi_args: if opts.pi_args.is_empty() {
            saved.pi_args
        } else {
            opts.pi_args
        },
        model: opts.model.or(saved.model),
        thinking: opts.thinking.or(saved.thinking),
        cwd,
        no_builtin_tools: opts.no_builtin_tools.unwrap_or(saved.no_builtin_tools),
        user_extensions: opts.user_extensions.unwrap_or(saved.user_extensions),
        debounce_ms: opts.debounce_ms.or(saved.debounce_ms),
        debounce_max_ms: opts.debounce_max_ms.or(saved.debounce_max_ms),
    };
    let mut pi_args = cfg.pi_args.clone();
    if !cfg.user_extensions {
        // `--no-iso` is a flag registered by the iso extension; with extensions off pi would
        // reject it as unknown. Older sessions were started with it.
        pi_args.retain(|a| a != "--no-iso");
    }
    store::write_json(&file, &cfg)?;

    let broker = Broker::open(Config {
        session: session.clone(),
        pi_bin: cfg.pi_bin,
        pi_args,
        model: cfg.model,
        thinking: cfg.thinking,
        cwd: cfg.cwd,
        no_builtin_tools: cfg.no_builtin_tools,
        user_extensions: cfg.user_extensions,
        debounce: Duration::from_millis(cfg.debounce_ms.unwrap_or(1000)),
        debounce_max: Duration::from_millis(cfg.debounce_max_ms.unwrap_or(5000)),
    })?;
    let listener = server::bind(broker.socket_path()).await?;
    tokio::spawn(server::serve(broker.clone(), listener));
    let api_file = session.join("api.json");
    let _ = std::fs::remove_file(&api_file);
    if !opts.no_http {
        let token = uuid::Uuid::new_v4().simple().to_string();
        let addr = api::serve(broker.clone(), opts.http, token.clone()).await?;
        let url = format!("http://{addr}");
        write_private(
            &api_file,
            &serde_json::to_vec_pretty(&serde_json::json!({ "url": url, "token": token }))?,
        )?;
        eprintln!("agentcord: api {url} (token in {})", api_file.display());
    }
    eprintln!(
        "agentcord: agents run with {}, {}",
        if cfg.no_builtin_tools {
            "agentcord tools only (no pi built-in tools)"
        } else {
            "pi built-in tools"
        },
        if cfg.user_extensions {
            "your pi extensions"
        } else {
            "no user pi extensions"
        },
    );
    eprintln!(
        "agentcord: {} session {}",
        if resumed { "resumed" } else { "new" },
        session.display()
    );
    eprintln!("agentcord: export AGENTCORD_SESSION={}", session.display());
    let woken = broker.resume();
    if !woken.is_empty() {
        eprintln!(
            "agentcord: delivering queued messages to {}",
            woken.join(", ")
        );
    }
    Ok(broker)
}

/// Write a file only the owner can read (the API token lives in it).
fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    f.write_all(bytes)?;
    Ok(())
}

async fn wait_for_exit(broker: &Broker) {
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = broker.shutdown.notified() => {}
    }
}

async fn run(
    session: Option<PathBuf>,
    opts: ServeOpts,
    name: Option<String>,
    exit_when_idle: bool,
    verbose: bool,
    prompt: String,
) -> Result<()> {
    let broker = start(session, opts).await?;
    let printer = Printer::new(verbose);
    let mut rx = broker.feed.subscribe();
    tokio::spawn(async move {
        while let Ok(ev) = rx.recv().await {
            if let Some(line) = printer.render(&ev) {
                println!("{line}");
            }
        }
    });

    let resp = broker.handle(Request::Spawn {
        from: HUMAN.into(),
        prompt,
        name,
        model: None,
        thinking: None,
        cwd: None,
        tools: None,
        system: None,
        report: Some(true),
    });
    if !resp.ok {
        bail!("{}", resp.error.unwrap_or_default());
    }
    let root = resp
        .data
        .as_ref()
        .and_then(|d| d["id"].as_str())
        .unwrap_or_default()
        .to_string();
    if !exit_when_idle {
        eprintln!(
            "agentcord: type to DM {root}; '@agent text' DMs another agent; '#topic text' posts; \
             /agents /topics /read <log> [before] /kill <agent> /quit"
        );
    }

    let input = {
        let broker = broker.clone();
        async move {
            let mut lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let req = match parse_input(line, &root) {
                    Some(Ok(req)) => req,
                    Some(Err(())) => return,
                    None => {
                        eprintln!("?");
                        continue;
                    }
                };
                let resp = broker.handle(req);
                // Sent messages already show up in the feed; only print command output.
                match (resp.ok, resp.text, resp.error) {
                    (true, Some(text), _) if line.starts_with('/') => println!("{text}"),
                    (false, _, Some(e)) => eprintln!("error: {e}"),
                    _ => {}
                }
            }
            // stdin closed: keep running until interrupted.
            std::future::pending::<()>().await
        }
    };

    if exit_when_idle {
        let idle = {
            let broker = broker.clone();
            async move {
                let mut quiet = 0;
                loop {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    quiet = if broker.is_quiescent() { quiet + 1 } else { 0 };
                    if quiet >= 6 {
                        return;
                    }
                }
            }
        };
        tokio::select! {
            _ = idle => eprintln!("agentcord: all agents idle, exiting"),
            _ = wait_for_exit(&broker) => {}
        }
    } else {
        tokio::select! {
            _ = input => {}
            _ = wait_for_exit(&broker) => {}
        }
    }
    broker.stop().await;
    Ok(())
}

/// Interactive input for `run`. `Some(Err(()))` means quit.
fn parse_input(line: &str, root: &str) -> Option<Result<Request, ()>> {
    let from = HUMAN.to_string();
    let (head, rest) = line
        .split_once(char::is_whitespace)
        .map_or((line, ""), |(h, r)| (h, r.trim()));
    Some(Ok(match head {
        "/quit" | "/exit" => return Some(Err(())),
        "/agents" => Request::Agents { from },
        "/topics" => Request::Topics { from },
        "/kill" => Request::Kill {
            from,
            agent: rest.to_string(),
        },
        "/read" => {
            let (log, before) = rest
                .split_once(' ')
                .map_or((rest, None), |(l, b)| (l, b.trim().parse().ok()));
            Request::Read {
                from,
                log: log.to_string(),
                before,
                after: None,
                limit: Some(20),
            }
        }
        h if h.starts_with('/') => return None,
        h if h.starts_with('@') && !rest.is_empty() => Request::Send {
            from,
            to: h.to_string(),
            text: rest.to_string(),
        },
        h if h.starts_with('#') && !rest.is_empty() => Request::Post {
            from,
            topic: h.to_string(),
            text: rest.to_string(),
        },
        _ => Request::Send {
            from,
            to: root.to_string(),
            text: line.to_string(),
        },
    }))
}

async fn client(session: Option<PathBuf>, cmd: Cmd) -> Result<()> {
    let session = match session {
        Some(s) => s,
        None => latest_session()?,
    };
    let socket = session.join("broker.sock");
    let from = HUMAN.to_string();
    let req = match cmd {
        Cmd::Spawn {
            name,
            model,
            thinking,
            cwd,
            prompt,
        } => Request::Spawn {
            from,
            prompt: prompt.join(" "),
            name,
            model,
            thinking,
            cwd,
            tools: None,
            system: None,
            report: Some(true),
        },
        Cmd::Send { to, text } => Request::Send {
            from,
            to,
            text: text.join(" "),
        },
        Cmd::Post { topic, text } => Request::Post {
            from,
            topic,
            text: text.join(" "),
        },
        Cmd::Topic {
            name,
            description,
            invite,
        } => Request::TopicCreate {
            from,
            name,
            description,
            invite,
        },
        Cmd::RenameTopic { topic, name } => Request::TopicName { from, topic, name },
        Cmd::Topics => Request::Topics { from },
        Cmd::Agents => Request::Agents { from },
        Cmd::Read {
            log,
            before,
            after,
            limit,
        } => Request::Read {
            from,
            log,
            before,
            after,
            limit,
        },
        Cmd::Kill { agent } => Request::Kill { from, agent },
        Cmd::Shutdown => Request::Shutdown,
        Cmd::Watch { verbose } => {
            let printer = Printer::new(verbose);
            return server::watch(&socket, |ev| {
                if let Some(line) = printer.render(&ev) {
                    println!("{line}");
                }
            })
            .await;
        }
        Cmd::Serve(_) | Cmd::Run { .. } => unreachable!(),
    };
    let resp = server::request(&socket, &req).await?;
    if !resp.ok {
        bail!("{}", resp.error.unwrap_or_default());
    }
    println!("{}", resp.text.unwrap_or_default().trim_end());
    Ok(())
}

fn latest_session() -> Result<PathBuf> {
    let dir = Path::new(SESSIONS);
    let mut all: Vec<PathBuf> = std::fs::read_dir(dir)
        .with_context(|| format!("no --session given and no {SESSIONS} directory here"))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.join("session.json").exists())
        .collect();
    all.sort();
    all.pop().context("no sessions found")
}

/// Renders feed events as one-line (messages: multi-line) terminal output.
struct Printer {
    verbose: bool,
    color: bool,
}

impl Printer {
    fn new(verbose: bool) -> Self {
        Self {
            verbose,
            color: std::io::stdout().is_terminal(),
        }
    }

    fn paint(&self, code: &str, s: &str) -> String {
        if self.color {
            format!("\x1b[{code}m{s}\x1b[0m")
        } else {
            s.to_string()
        }
    }

    fn render(&self, ev: &Feed) -> Option<String> {
        let time = chrono::Local::now().format("%H:%M:%S").to_string();
        let t = self.paint("2", &time);
        Some(match ev {
            Feed::Message {
                msg,
                title,
                who,
                to,
            } => {
                let kind = match msg.kind {
                    MsgKind::Message => String::new(),
                    k => format!(" [{}]", serde_json::to_value(k).ok()?.as_str()?),
                };
                let to = if to.is_empty() {
                    String::new()
                } else {
                    format!(" → {}", to.join(", "))
                };
                let head = self.paint("1;36", &format!("{title} #{} · {who}{kind}{to}", msg.seq));
                let body: String = msg.text.lines().map(|l| format!("\n    {l}")).collect();
                format!("{t} {head}{body}")
            }
            Feed::AgentCreated { who, parent, .. } => {
                format!(
                    "{t} {}",
                    self.paint("1;32", &format!("+ {who} spawned by {parent}"))
                )
            }
            Feed::AgentStatus { who, status, .. } => {
                if !self.verbose && !matches!(status, Status::Killed) {
                    return None;
                }
                format!(
                    "{t} {}",
                    self.paint("2", &format!("{who} is {status:?}").to_lowercase())
                )
            }
            Feed::AssistantText { who, text, .. } => {
                if !self.verbose {
                    return None;
                }
                format!(
                    "{t} {} {}",
                    self.paint("35", &format!("{who}:")),
                    broker::truncate(text, 600)
                )
            }
            Feed::ToolStart {
                who, tool, args, ..
            } => {
                format!(
                    "{t} {} {}",
                    self.paint("33", &format!("{who} ⚙ {tool}")),
                    self.paint("2", args)
                )
            }
            Feed::ToolEnd {
                who,
                tool,
                is_error,
                ..
            } => {
                if !*is_error && !self.verbose {
                    return None;
                }
                let mark = if *is_error {
                    self.paint("31", "✗")
                } else {
                    self.paint("32", "✓")
                };
                format!("{t} {who} {mark} {tool}")
            }
            Feed::NameClaimed { id, name } => {
                format!("{t} {}", self.paint("32", &format!("{id} is now @{name}")))
            }
            Feed::TopicCreated { topic, by, .. } => {
                format!(
                    "{t} {}",
                    self.paint("32", &format!("+ topic {topic} created by {by}"))
                )
            }
            Feed::TopicNamed { id, name, by } => format!(
                "{t} {}",
                self.paint("32", &format!("topic {id} named #{name} by {by}"))
            ),
            Feed::Subscribed { who, topic, mode } => {
                format!(
                    "{t} {}",
                    self.paint(
                        "2",
                        &format!("{who} subscribed to {topic} ({mode:?})").to_lowercase()
                    )
                )
            }
            Feed::Unsubscribed { who, topic } => format!(
                "{t} {}",
                self.paint("2", &format!("{who} unsubscribed from {topic}"))
            ),
            Feed::Error { who, error } => {
                format!("{t} {}", self.paint("31", &format!("{who}: {error}")))
            }
        })
    }
}
