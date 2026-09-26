//! HTTP and WebSocket client for the agentcord daemon API.

use agentcord_proto::{Response, Snapshot, WsFrame};
use anyhow::{Context, Result, bail};
use futures_util::StreamExt;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message as WsMessage;

#[derive(Clone)]
pub struct Api {
    pub url: String,
    token: String,
    http: reqwest::Client,
}

impl Api {
    pub fn new(url: String, token: String) -> Self {
        Self {
            url: url.trim_end_matches('/').to_string(),
            token,
            http: reqwest::Client::new(),
        }
    }

    pub async fn state(&self) -> Result<Snapshot> {
        let resp: Response = self
            .http
            .get(format!("{}/api/state", self.url))
            .bearer_auth(&self.token)
            .send()
            .await
            .context("daemon unreachable")?
            .json()
            .await?;
        if !resp.ok {
            bail!("{}", resp.error.unwrap_or_default());
        }
        Ok(serde_json::from_value(resp.data.unwrap_or_default())?)
    }

    /// POST an op (`spawn`, `send`, `post`, `topic`, `topic/rename`, `kill`). Returns the
    /// broker's response; `Err` only for transport failures and broker errors.
    pub async fn call(&self, path: &str, body: Value) -> Result<Response> {
        let resp: Response = self
            .http
            .post(format!("{}/api/{path}", self.url))
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await
            .context("daemon unreachable")?
            .json()
            .await?;
        if !resp.ok {
            bail!("{}", resp.error.unwrap_or_default());
        }
        Ok(resp)
    }

    fn ws_url(&self, query: &[(&str, &str)]) -> String {
        let base = self.url.replacen("http", "ws", 1);
        let mut url = format!("{base}/ws?token={}", encode(&self.token));
        for (k, v) in query {
            url.push_str(&format!("&{k}={}", encode(v)));
        }
        url
    }

    /// Stream WebSocket frames into `tx`, tagged with `tag`, until the socket or `tx` closes.
    pub async fn stream<T: Clone + Send + 'static>(
        &self,
        query: &[(&str, &str)],
        tag: T,
        tx: mpsc::UnboundedSender<(T, WsFrame)>,
    ) -> Result<()> {
        let (ws, _) = tokio_tungstenite::connect_async(self.ws_url(query)).await?;
        let (_, mut rx) = ws.split();
        while let Some(msg) = rx.next().await {
            let text = match msg? {
                WsMessage::Text(t) => t,
                WsMessage::Close(_) => break,
                _ => continue,
            };
            if let Ok(frame) = serde_json::from_str::<WsFrame>(&text)
                && tx.send((tag.clone(), frame)).is_err()
            {
                break;
            }
        }
        Ok(())
    }
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
