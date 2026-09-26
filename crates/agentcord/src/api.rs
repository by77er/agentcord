//! HTTP + WebSocket API over the broker, for the TUI and scripts. Every request acts as `@human`.
//!
//! Auth: a per-run token (in `<session>/api.json`) as `Authorization: Bearer <token>` or `?token=`.
//!
//! ```text
//! GET  /api/state                       Snapshot { agents, logs }
//! GET  /api/agents | /api/topics        listings (text + data)
//! GET  /api/read?log=&before=&after=&limit=
//! POST /api/spawn         {prompt, name?, model?, thinking?, cwd?, tools?, system?}
//! POST /api/send          {to, text}
//! POST /api/post          {topic, text}
//! POST /api/topic         {name?, description?, invite?}
//! POST /api/topic/rename  {topic, name}
//! POST /api/topic/invite  {topic, agents}
//! POST /api/kill          {agent}
//! GET  /ws                    global activity feed (WsFrame::Feed)
//! GET  /ws?agent=<id|@name>   one agent's flow: backlog, `synced`, then live (incl. deltas)
//! GET  /ws?log=<#topic|t-id|dm:..|@agent>  one log's messages: backlog, `synced`, then live
//! ```

use crate::broker::Broker;
use agentcord_proto::{Feed, HUMAN, Request, Response, WsFrame};
use anyhow::Result;
use axum::extract::ws::{Message as WsMessage, WebSocketUpgrade};
use axum::extract::{Query, Request as HttpRequest, State};
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::broadcast::error::RecvError;

#[derive(Clone)]
struct Api {
    broker: Arc<Broker>,
    token: Arc<str>,
}

pub async fn serve(broker: Arc<Broker>, addr: SocketAddr, token: String) -> Result<SocketAddr> {
    let api = Api {
        broker,
        token: token.into(),
    };
    let op = |name: &'static str| {
        post(move |State(api): State<Api>, Json(body): Json<Value>| call(api, name, body))
    };
    let app = Router::new()
        .route(
            "/api/state",
            get(|State(api): State<Api>| call(api, "state", json!({}))),
        )
        .route(
            "/api/agents",
            get(|State(api): State<Api>| call(api, "agents", json!({}))),
        )
        .route(
            "/api/topics",
            get(|State(api): State<Api>| call(api, "topics", json!({}))),
        )
        .route(
            "/api/read",
            get(|State(api): State<Api>, Query(q): Query<Value>| call(api, "read", q)),
        )
        .route("/api/spawn", op("spawn"))
        .route("/api/send", op("send"))
        .route("/api/post", op("post"))
        .route("/api/topic", op("topic_create"))
        .route("/api/topic/rename", op("topic_name"))
        .route("/api/topic/invite", op("invite"))
        .route("/api/kill", op("kill"))
        .route("/ws", get(ws))
        .layer(middleware::from_fn_with_state(api.clone(), auth))
        .with_state(api);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let local = listener.local_addr()?;
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            eprintln!("agentcord: api server stopped: {e}");
        }
    });
    Ok(local)
}

async fn auth(State(api): State<Api>, req: HttpRequest, next: Next) -> axum::response::Response {
    let header = req
        .headers()
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "));
    let query = req
        .uri()
        .query()
        .unwrap_or("")
        .split('&')
        .find_map(|kv| kv.strip_prefix("token="));
    if header.or(query) == Some(&*api.token) {
        next.run(req).await
    } else {
        (
            StatusCode::UNAUTHORIZED,
            Json(Response::err("missing or bad token")),
        )
            .into_response()
    }
}

/// Run a broker op as `@human`. Query strings arrive as strings, so numeric fields are coerced.
async fn call(api: Api, op: &str, mut body: Value) -> impl IntoResponse {
    if !body.is_object() {
        body = json!({});
    }
    body["op"] = json!(op);
    body["from"] = json!(HUMAN);
    for key in ["before", "after", "limit"] {
        if let Some(n) = body[key].as_str().and_then(|s| s.parse::<u64>().ok()) {
            body[key] = json!(n);
        }
    }
    let resp = match serde_json::from_value::<Request>(body) {
        Ok(req) => api.broker.handle(req),
        Err(e) => Response::err(format!("bad request: {e}")),
    };
    let status = if resp.ok {
        StatusCode::OK
    } else {
        StatusCode::BAD_REQUEST
    };
    (status, Json(resp))
}

#[derive(Deserialize)]
struct WsQuery {
    agent: Option<String>,
    log: Option<String>,
    backlog: Option<usize>,
}

async fn ws(
    State(api): State<Api>,
    Query(q): Query<WsQuery>,
    up: WebSocketUpgrade,
) -> impl IntoResponse {
    up.on_upgrade(move |socket| async move {
        let (mut tx, mut rx) = socket.split();
        // Drain client frames so closes are noticed; the stream itself is one-way.
        let closed = async move {
            while let Some(Ok(msg)) = rx.next().await {
                if matches!(msg, WsMessage::Close(_)) {
                    break;
                }
            }
        };
        let pump = pump(&api.broker, q, &mut tx);
        tokio::select! {
            _ = closed => {}
            _ = pump => {}
        }
    })
}

async fn send(tx: &mut (impl SinkExt<WsMessage> + Unpin), frame: &WsFrame) -> bool {
    let text = serde_json::to_string(frame).unwrap_or_default();
    tx.send(WsMessage::Text(text.into())).await.is_ok()
}

async fn pump(broker: &Arc<Broker>, q: WsQuery, tx: &mut (impl SinkExt<WsMessage> + Unpin)) {
    if let Some(spec) = q.agent {
        let (id, backlog, mut live) = match broker.subscribe_flow(&spec, q.backlog.unwrap_or(500)) {
            Ok(sub) => sub,
            Err(e) => {
                send(
                    tx,
                    &WsFrame::Error {
                        error: format!("{e:#}"),
                    },
                )
                .await;
                return;
            }
        };
        for item in backlog {
            if !send(
                tx,
                &WsFrame::Flow {
                    agent: id.clone(),
                    live: false,
                    item,
                },
            )
            .await
            {
                return;
            }
        }
        if !send(tx, &WsFrame::Synced).await {
            return;
        }
        loop {
            match live.recv().await {
                Ok((agent, item)) if agent == id => {
                    if !send(
                        tx,
                        &WsFrame::Flow {
                            agent,
                            live: true,
                            item,
                        },
                    )
                    .await
                    {
                        return;
                    }
                }
                Ok(_) => {}
                Err(RecvError::Lagged(n)) => {
                    let error = format!("stream lagged; {n} items dropped");
                    if !send(tx, &WsFrame::Error { error }).await {
                        return;
                    }
                }
                Err(RecvError::Closed) => return,
            }
        }
    } else if let Some(spec) = q.log {
        let (log, backlog, mut live) = match broker.subscribe_log(&spec, q.backlog.unwrap_or(200)) {
            Ok(sub) => sub,
            Err(e) => {
                send(
                    tx,
                    &WsFrame::Error {
                        error: format!("{e:#}"),
                    },
                )
                .await;
                return;
            }
        };
        for (who, msg) in backlog {
            if !send(
                tx,
                &WsFrame::Message {
                    live: false,
                    who,
                    msg,
                },
            )
            .await
            {
                return;
            }
        }
        if !send(tx, &WsFrame::Synced).await {
            return;
        }
        loop {
            match live.recv().await {
                Ok(Feed::Message { msg, who, .. }) if msg.log == log => {
                    if !send(
                        tx,
                        &WsFrame::Message {
                            live: true,
                            who,
                            msg,
                        },
                    )
                    .await
                    {
                        return;
                    }
                }
                Ok(_) | Err(RecvError::Lagged(_)) => {}
                Err(RecvError::Closed) => return,
            }
        }
    } else {
        let mut live = broker.feed.subscribe();
        if !send(tx, &WsFrame::Synced).await {
            return;
        }
        loop {
            match live.recv().await {
                Ok(event) => {
                    if !send(tx, &WsFrame::Feed { event }).await {
                        return;
                    }
                }
                Err(RecvError::Lagged(_)) => {}
                Err(RecvError::Closed) => return,
            }
        }
    }
}
