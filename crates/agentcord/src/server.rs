//! Unix socket server and client. One JSON request per line, one JSON response per line;
//! a `watch` request turns the connection into a stream of `Feed` events.

use crate::broker::Broker;
use agentcord_proto::{Feed, Request, Response};
use anyhow::{Context, Result, bail};
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::broadcast::error::RecvError;

pub async fn bind(path: &Path) -> Result<UnixListener> {
    if path.exists() {
        if UnixStream::connect(path).await.is_ok() {
            bail!(
                "a broker is already serving this session ({})",
                path.display()
            );
        }
        std::fs::remove_file(path)?;
    }
    if path.as_os_str().len() > 100 {
        eprintln!(
            "agentcord: warning: socket path is long and may exceed the OS limit: {}",
            path.display()
        );
    }
    UnixListener::bind(path).with_context(|| format!("binding {}", path.display()))
}

pub async fn serve(broker: Arc<Broker>, listener: UnixListener) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let broker = broker.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_conn(broker, stream).await {
                eprintln!("agentcord: connection error: {e:#}");
            }
        });
    }
}

async fn handle_conn(broker: Arc<Broker>, stream: UnixStream) -> Result<()> {
    let (r, mut w) = stream.into_split();
    let mut lines = BufReader::new(r).lines();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let resp = match serde_json::from_str::<Request>(&line) {
            Ok(Request::Watch) => {
                let mut rx = broker.feed.subscribe();
                loop {
                    let ev = match rx.recv().await {
                        Ok(ev) => ev,
                        Err(RecvError::Lagged(_)) => continue,
                        Err(RecvError::Closed) => return Ok(()),
                    };
                    let mut out = serde_json::to_string(&ev)?;
                    out.push('\n');
                    if w.write_all(out.as_bytes()).await.is_err() {
                        return Ok(());
                    }
                }
            }
            Ok(req) => broker.handle(req),
            Err(e) => Response::err(format!("bad request: {e}")),
        };
        let mut out = serde_json::to_string(&resp)?;
        out.push('\n');
        w.write_all(out.as_bytes()).await?;
    }
    Ok(())
}

pub async fn request(socket: &Path, req: &Request) -> Result<Response> {
    let stream = UnixStream::connect(socket).await.with_context(|| {
        format!(
            "no broker at {} (is `agentcord serve` running?)",
            socket.display()
        )
    })?;
    let (r, mut w) = stream.into_split();
    let mut line = serde_json::to_string(req)?;
    line.push('\n');
    w.write_all(line.as_bytes()).await?;
    let mut lines = BufReader::new(r).lines();
    let resp = lines
        .next_line()
        .await?
        .context("broker closed the connection")?;
    Ok(serde_json::from_str(&resp)?)
}

/// Stream feed events from a broker until it goes away.
pub async fn watch(socket: &Path, mut on_event: impl FnMut(Feed)) -> Result<()> {
    let stream = UnixStream::connect(socket)
        .await
        .with_context(|| format!("no broker at {}", socket.display()))?;
    let (r, mut w) = stream.into_split();
    w.write_all(b"{\"op\":\"watch\"}\n").await?;
    let mut lines = BufReader::new(r).lines();
    while let Some(line) = lines.next_line().await? {
        if let Ok(ev) = serde_json::from_str(&line) {
            on_event(ev);
        }
    }
    Ok(())
}
