//! Minimal client for the agentcord daemon's HTTP/WebSocket API.

use agentcord_proto::{LogKind, Response, Snapshot, WsFrame};
use anyhow::{Context, Result, bail};
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::path::PathBuf;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

const SESSIONS: &str = ".agentcord/sessions";

pub struct Daemon {
    url: String,
    token: String,
    http: reqwest::Client,
}

pub struct LogStream(WebSocketStream<MaybeTlsStream<TcpStream>>);

impl Daemon {
    /// From an explicit URL + token, or `<session>/api.json` (newest session by default).
    pub fn connect(
        url: Option<String>,
        token: Option<String>,
        session: Option<PathBuf>,
    ) -> Result<Self> {
        let (url, token) = match (url, token) {
            (Some(u), Some(t)) => (u, t),
            _ => {
                let session = match session {
                    Some(s) => s,
                    None => latest_session()?,
                };
                let path = session.join("api.json");
                let v: Value =
                    serde_json::from_slice(&std::fs::read(&path).with_context(|| {
                        format!("reading {} (is the daemon running?)", path.display())
                    })?)?;
                let url = v["url"].as_str().context("api.json: url")?.to_string();
                let token = v["token"].as_str().context("api.json: token")?.to_string();
                (url, token)
            }
        };
        Ok(Self {
            url: url.trim_end_matches('/').to_string(),
            token,
            http: reqwest::Client::new(),
        })
    }

    async fn call(&self, req: reqwest::RequestBuilder) -> Result<Value> {
        let resp: Response = req
            .bearer_auth(&self.token)
            .send()
            .await
            .context("agentcord daemon unreachable")?
            .json()
            .await?;
        if !resp.ok {
            bail!("{}", resp.error.unwrap_or_default());
        }
        Ok(resp.data.unwrap_or_default())
    }

    /// Resolve `#name`/`t-...` to (topic id, title), creating the topic if it doesn't exist.
    pub async fn ensure_topic(&self, spec: &str) -> Result<(String, String)> {
        let spec = spec.trim().trim_start_matches('#');
        let find = |snap: &Snapshot| {
            snap.logs
                .iter()
                .find(|l| {
                    l.kind == LogKind::Topic && (l.log == spec || l.name.as_deref() == Some(spec))
                })
                .map(|l| (l.log.clone(), l.title.clone()))
        };
        let snap: Snapshot = serde_json::from_value(
            self.call(self.http.get(format!("{}/api/state", self.url)))
                .await?,
        )?;
        if let Some(found) = find(&snap) {
            return Ok(found);
        }
        if spec.starts_with("t-") {
            bail!("no topic {spec}");
        }
        let body = json!({ "name": spec, "description": "Mirrored to Discord" });
        self.call(
            self.http
                .post(format!("{}/api/topic", self.url))
                .json(&body),
        )
        .await?;
        let snap: Snapshot = serde_json::from_value(
            self.call(self.http.get(format!("{}/api/state", self.url)))
                .await?,
        )?;
        find(&snap).context("topic vanished after creating it")
    }

    /// Post to a topic as @human.
    pub async fn post(&self, topic: &str, text: &str) -> Result<()> {
        let body = json!({ "topic": topic, "text": text });
        self.call(self.http.post(format!("{}/api/post", self.url)).json(&body))
            .await?;
        Ok(())
    }

    pub async fn log_stream(&self, log: &str, backlog: usize) -> Result<LogStream> {
        let url = format!(
            "{}/ws?token={}&log={}&backlog={backlog}",
            self.url.replacen("http", "ws", 1),
            encode(&self.token),
            encode(log)
        );
        let (ws, _) = tokio_tungstenite::connect_async(url)
            .await
            .context("connecting to the topic stream")?;
        Ok(LogStream(ws))
    }
}

impl LogStream {
    pub async fn next_frame(&mut self) -> Result<Option<WsFrame>> {
        while let Some(msg) = self.0.next().await {
            match msg? {
                WsMessage::Text(t) => return Ok(Some(serde_json::from_str(&t)?)),
                WsMessage::Close(_) => return Ok(None),
                _ => {}
            }
        }
        Ok(None)
    }
}

fn latest_session() -> Result<PathBuf> {
    let mut all: Vec<PathBuf> = std::fs::read_dir(SESSIONS)
        .with_context(|| format!("no --session/--url given and no {SESSIONS} directory here"))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.join("api.json").exists())
        .collect();
    all.sort();
    all.pop().context("no running session found")
}

fn encode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}
