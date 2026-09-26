//! Converts pi RPC events into an agent's flow (`FlowItem`s), both live and when replaying
//! `rpc.jsonl` for a backlog.

use agentcord_proto::{FlowItem, Status, ToolCall};
use serde_json::Value;

const MAX_ARGS: usize = 4_000;
const MAX_RESULT: usize = 8_000;

pub fn from_pi_event(ev: &Value) -> Option<FlowItem> {
    Some(match ev["type"].as_str()? {
        "agent_start" => FlowItem::Status {
            status: Status::Running,
        },
        "agent_settled" => FlowItem::Status {
            status: Status::Idle,
        },
        "message_update" => {
            let e = &ev["assistantMessageEvent"];
            let thinking = match e["type"].as_str()? {
                "text_delta" => false,
                "thinking_delta" => true,
                _ => return None,
            };
            FlowItem::Delta {
                thinking,
                delta: e["delta"].as_str()?.to_string(),
            }
        }
        "message_end" => {
            let m = &ev["message"];
            match m["role"].as_str()? {
                "user" => FlowItem::Inbound {
                    text: blocks_text(&m["content"], "text"),
                },
                "assistant" => {
                    let tool_calls = m["content"]
                        .as_array()
                        .map(|blocks| {
                            blocks
                                .iter()
                                .filter(|b| b["type"] == "toolCall")
                                .map(|b| ToolCall {
                                    id: b["id"].as_str().unwrap_or_default().to_string(),
                                    name: b["name"].as_str().unwrap_or_default().to_string(),
                                    args: clip(&b["arguments"].to_string(), MAX_ARGS),
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    FlowItem::Assistant {
                        text: blocks_text(&m["content"], "text"),
                        thinking: blocks_text(&m["content"], "thinking"),
                        tool_calls,
                        stop_reason: m["stopReason"].as_str().unwrap_or_default().to_string(),
                        error: m["errorMessage"].as_str().map(str::to_string),
                    }
                }
                _ => return None,
            }
        }
        "tool_execution_start" => FlowItem::ToolStart {
            call_id: ev["toolCallId"].as_str().unwrap_or_default().to_string(),
            tool: ev["toolName"].as_str().unwrap_or_default().to_string(),
            args: clip(&ev["args"].to_string(), MAX_ARGS),
        },
        "tool_execution_end" => FlowItem::ToolEnd {
            call_id: ev["toolCallId"].as_str().unwrap_or_default().to_string(),
            tool: ev["toolName"].as_str().unwrap_or_default().to_string(),
            result: clip(&blocks_text(&ev["result"]["content"], "text"), MAX_RESULT),
            is_error: ev["isError"].as_bool().unwrap_or(false),
        },
        "compaction_start" => FlowItem::Note {
            text: format!(
                "compacting context ({})",
                ev["reason"].as_str().unwrap_or("?")
            ),
        },
        "compaction_end" => FlowItem::Note {
            text: "compaction finished".into(),
        },
        "auto_retry_start" => FlowItem::Note {
            text: format!(
                "retrying after error (attempt {}): {}",
                ev["attempt"],
                ev["errorMessage"].as_str().unwrap_or("?")
            ),
        },
        "extension_error" => FlowItem::Note {
            text: format!("extension error: {}", ev["error"].as_str().unwrap_or("?")),
        },
        _ => return None,
    })
}

/// Text of a pi content value: a plain string, or the `kind` blocks of a block array.
fn blocks_text(content: &Value, kind: &str) -> String {
    match content {
        Value::String(s) if kind == "text" => s.clone(),
        Value::Array(blocks) => blocks
            .iter()
            .filter(|b| b["type"] == kind)
            .filter_map(|b| b[kind].as_str())
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

fn clip(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}… [{} more bytes]", &s[..end], s.len() - end)
}
