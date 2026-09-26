//! Drawing.

use crate::app::{App, Entry, Focus, View, ViewKind, is_human_dm, short, short_title};
use agentcord_proto::{FlowItem, HUMAN, LogKind, MsgKind, Status};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style, Stylize};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Clear, List, ListItem, ListState, Paragraph, Wrap};
use unicode_width::UnicodeWidthChar;

pub fn draw(f: &mut Frame, app: &mut App) {
    let [main, input, status] = Layout::vertical([
        Constraint::Min(6),
        Constraint::Length(3),
        Constraint::Length(1),
    ])
    .areas(f.area());
    let left_w = (main.width * 35 / 100).clamp(30, 52);
    let [left, right] =
        Layout::horizontal([Constraint::Length(left_w), Constraint::Min(20)]).areas(main);
    let [agents, logs, activity] = Layout::vertical([
        Constraint::Percentage(40),
        Constraint::Percentage(30),
        Constraint::Percentage(30),
    ])
    .areas(left);

    draw_agents(f, app, agents);
    draw_logs(f, app, logs);
    draw_activity(f, app, activity);
    draw_view(f, app, right);
    draw_input(f, app, input);
    draw_status(f, app, status);
    if app.show_help {
        draw_help(f);
    }
}

fn block(title: impl Into<Line<'static>>, focused: bool) -> Block<'static> {
    let color = if focused {
        Color::Cyan
    } else {
        Color::DarkGray
    };
    Block::bordered()
        .border_type(if focused {
            BorderType::Thick
        } else {
            BorderType::Rounded
        })
        .border_style(Style::new().fg(color))
        .title(title)
}

fn status_glyph(s: Status) -> (&'static str, Color) {
    match s {
        Status::Running => ("●", Color::Green),
        Status::Idle => ("○", Color::Blue),
        Status::Dormant => ("◌", Color::DarkGray),
        Status::Killed => ("✗", Color::Red),
    }
}

fn viewing(app: &App) -> Option<&ViewKind> {
    app.view.as_ref().map(|v| &v.kind)
}

fn draw_agents(f: &mut Frame, app: &App, area: Rect) {
    let rows = app.agent_rows();
    let running = rows
        .iter()
        .filter(|(_, a)| a.status == Status::Running)
        .count();
    let items: Vec<ListItem> = rows
        .iter()
        .map(|(depth, a)| {
            let (glyph, color) = status_glyph(a.status);
            let open = viewing(app) == Some(&ViewKind::Agent(a.id.clone()));
            let name = a.name.as_ref().map_or(a.id.clone(), |n| format!("@{n}"));
            let mut spans = vec![
                Span::raw("  ".repeat(*depth)),
                Span::styled(glyph, Style::new().fg(color)),
                Span::raw(" "),
                Span::styled(
                    name,
                    if open {
                        Style::new().bold().underlined()
                    } else {
                        Style::new().bold()
                    },
                ),
                Span::styled(
                    format!(" {}", format!("{:?}", a.status).to_lowercase()),
                    Style::new().fg(Color::DarkGray),
                ),
            ];
            if a.pending > 0 {
                spans.push(Span::styled(
                    format!(" {}✉", a.pending),
                    Style::new().fg(Color::Yellow),
                ));
            }
            ListItem::new(Line::from(spans))
        })
        .collect();
    let title = format!(" Agents {} · {running} running ", rows.len());
    list(
        f,
        items,
        block(title, app.focus == Focus::Agents),
        app.agent_sel,
        app.focus == Focus::Agents,
        area,
    );
}

fn draw_logs(f: &mut Frame, app: &App, area: Rect) {
    let rows = app.log_rows();
    let items: Vec<ListItem> = rows
        .iter()
        .map(|l| {
            let open = viewing(app) == Some(&ViewKind::Log(l.log.clone()));
            let unread = app.unread.get(&l.log).copied().unwrap_or(0);
            let (name, color) = match l.kind {
                LogKind::Topic => (
                    l.name.as_ref().map_or(l.log.clone(), |n| format!("#{n}")),
                    Color::Reset,
                ),
                LogKind::Dm if is_human_dm(&l.log) => (
                    format!("✉ {}", short_title(&l.title).replace("DM ", "")),
                    Color::Magenta,
                ),
                LogKind::Dm => (
                    format!("✉ {}", short_title(&l.title).replace("DM ", "")),
                    Color::Gray,
                ),
            };
            let mut style = Style::new().fg(color);
            if open {
                style = style.underlined();
            }
            if unread > 0 {
                style = style.bold();
            }
            let mut spans = vec![
                Span::styled(name, style),
                Span::styled(format!(" {}", l.count), Style::new().fg(Color::DarkGray)),
            ];
            if l.kind == LogKind::Topic {
                spans.push(Span::styled(
                    format!(" · {} subs", l.members.len()),
                    Style::new().fg(Color::DarkGray),
                ));
            }
            if unread > 0 {
                spans.push(Span::styled(
                    format!(" +{unread}"),
                    Style::new().fg(Color::Yellow).bold(),
                ));
            }
            ListItem::new(Line::from(spans))
        })
        .collect();
    let title = format!(" Topics & DMs {} ", rows.len());
    list(
        f,
        items,
        block(title, app.focus == Focus::Logs),
        app.log_sel,
        app.focus == Focus::Logs,
        area,
    );
}

fn list(f: &mut Frame, items: Vec<ListItem>, block: Block, sel: usize, focused: bool, area: Rect) {
    let empty = items.is_empty();
    let highlight = if focused {
        Style::new().bg(Color::Cyan).fg(Color::Black)
    } else {
        Style::new().bg(Color::DarkGray)
    };
    let list = List::new(items).block(block).highlight_style(highlight);
    let mut state = ListState::default().with_selected((!empty).then_some(sel));
    f.render_stateful_widget(list, area, &mut state);
}

fn draw_activity(f: &mut Frame, app: &App, area: Rect) {
    let h = area.height.saturating_sub(2) as usize;
    let lines: Vec<Line> = app
        .activity
        .iter()
        .skip(app.activity.len().saturating_sub(h))
        .map(|(t, color, text)| {
            Line::from(vec![
                Span::styled(format!("{t} "), Style::new().fg(Color::DarkGray)),
                Span::styled(text.clone(), Style::new().fg(*color)),
            ])
        })
        .collect();
    f.render_widget(
        Paragraph::new(lines).block(block(" Activity ", false)),
        area,
    );
}

fn draw_view(f: &mut Frame, app: &mut App, area: Rect) {
    let Some(view) = &app.view else {
        let text = vec![
            Line::from("agentcord".bold()),
            Line::from(""),
            Line::from("Select an agent or topic (↑/↓, Enter) to watch it live."),
            Line::from("Start a swarm:  s  or  /spawn --name lead <task>"),
            Line::from("Talk as @human:  @agent text   #topic text   or just type in an open view"),
            Line::from(""),
            Line::from("? for all keys and commands".dark_gray()),
        ];
        let p = Paragraph::new(text)
            .block(block(" View ", false))
            .wrap(Wrap { trim: false });
        return f.render_widget(p, area);
    };

    let title = view_title(app, view);
    let inner_w = area.width.saturating_sub(2).max(1) as usize;
    let h = area.height.saturating_sub(2) as usize;
    let mut lines: Vec<Line> = Vec::new();
    for (style, text) in view_lines(view) {
        // Continuation lines keep the logical line's indent.
        let indent: String = text.chars().take_while(|c| *c == ' ').collect();
        let width = inner_w.saturating_sub(indent.len()).max(8);
        for (i, piece) in wrap(&text, width).into_iter().enumerate() {
            let piece = if i == 0 {
                piece
            } else {
                format!("{indent}{piece}")
            };
            lines.push(Line::styled(piece, style));
        }
    }
    app.view_height = h;
    let max_scroll = lines.len().saturating_sub(h);
    app.scroll = app.scroll.min(max_scroll);
    let start = lines.len().saturating_sub(h + app.scroll);
    let visible: Vec<Line> = lines.into_iter().skip(start).take(h).collect();
    let mut title = title;
    if app.scroll > 0 {
        title.push_str(&format!(" · ↑{} (End to follow)", app.scroll));
    }
    f.render_widget(
        Paragraph::new(visible).block(block(format!(" {title} "), false)),
        area,
    );
}

fn view_title(app: &App, view: &View) -> String {
    let loading = if view.synced { "" } else { " · loading…" };
    let err = view
        .error
        .as_ref()
        .map(|e| format!(" · {e}"))
        .unwrap_or_default();
    match &view.kind {
        ViewKind::Agent(id) => match app.agent(id) {
            Some(a) => format!(
                "{} · {} · {}{loading}{err}",
                a.label,
                format!("{:?}", a.status).to_lowercase(),
                a.model.as_deref().unwrap_or("default model")
            ),
            None => format!("{id}{loading}{err}"),
        },
        ViewKind::Log(log) => match app.log(log) {
            Some(l) => {
                let members: Vec<String> = l.members.iter().map(|m| short(m)).collect();
                let who = if l.kind == LogKind::Topic {
                    "subscribers"
                } else {
                    "between"
                };
                format!(
                    "{} · {who}: {}{loading}{err}",
                    short_title(&l.title),
                    members.join(", ")
                )
            }
            None => format!("{log}{loading}{err}"),
        },
    }
}

/// Logical (unwrapped) lines of a view.
fn view_lines(view: &View) -> Vec<(Style, String)> {
    let mut out: Vec<(Style, String)> = Vec::new();
    let dim = Style::new().fg(Color::DarkGray);
    let push_text = |out: &mut Vec<(Style, String)>, style: Style, prefix: &str, text: &str| {
        for l in text.lines() {
            out.push((style, format!("{prefix}{l}")));
        }
    };
    for e in &view.entries {
        match e {
            Entry::Flow(item) => match item {
                FlowItem::Status { status } => {
                    let (_, color) = status_glyph(*status);
                    out.push((
                        Style::new().fg(color),
                        format!("── {} ──", format!("{status:?}").to_lowercase()),
                    ));
                }
                FlowItem::Inbound { text } => {
                    out.push((Style::new().fg(Color::Cyan).bold(), "▶ into context".into()));
                    push_text(&mut out, Style::new().fg(Color::LightCyan), "  ", text);
                    out.push((Style::new(), String::new()));
                }
                FlowItem::Assistant {
                    text,
                    thinking,
                    error,
                    stop_reason,
                    ..
                } => {
                    if !thinking.trim().is_empty() {
                        push_text(&mut out, dim.italic(), "┆ ", thinking);
                    }
                    if !text.trim().is_empty() {
                        out.push((Style::new().fg(Color::Magenta).bold(), "◆ assistant".into()));
                        push_text(&mut out, Style::new(), "  ", text);
                    }
                    if let Some(e) = error {
                        out.push((Style::new().fg(Color::Red), format!("✗ {stop_reason}: {e}")));
                    }
                    if !text.trim().is_empty() || error.is_some() {
                        out.push((Style::new(), String::new()));
                    }
                }
                FlowItem::ToolStart { tool, args, .. } => {
                    let args: String = args
                        .chars()
                        .take(400)
                        .collect::<String>()
                        .replace('\n', " ");
                    out.push((Style::new().fg(Color::Yellow), format!("⚙ {tool} {args}")));
                }
                FlowItem::ToolEnd {
                    tool,
                    result,
                    is_error,
                    ..
                } => {
                    let (mark, color) = if *is_error {
                        ("✗", Color::Red)
                    } else {
                        ("✓", Color::Green)
                    };
                    out.push((Style::new().fg(color), format!("{mark} {tool}")));
                    let lines: Vec<&str> = result.lines().collect();
                    for l in lines.iter().take(6) {
                        out.push((dim, format!("  {l}")));
                    }
                    if lines.len() > 6 {
                        out.push((dim, format!("  … {} more lines", lines.len() - 6)));
                    }
                }
                FlowItem::Note { text } => out.push((dim.italic(), format!("· {text}"))),
                FlowItem::Delta { .. } => {}
            },
            Entry::Msg { who, msg } => {
                let (tag, color) = match msg.kind {
                    MsgKind::Task => (" [task]", Color::Cyan),
                    MsgKind::Report => (" [report]", Color::Green),
                    MsgKind::System => (" [system]", Color::Red),
                    MsgKind::Message if msg.from == HUMAN => ("", Color::Magenta),
                    MsgKind::Message => ("", Color::Yellow),
                };
                let ts = chrono::DateTime::parse_from_rfc3339(&msg.ts)
                    .map(|t| {
                        t.with_timezone(&chrono::Local)
                            .format("%H:%M:%S")
                            .to_string()
                    })
                    .unwrap_or_default();
                out.push((
                    Style::new().fg(color).bold(),
                    format!("{}{tag}  {ts} #{}", short(who), msg.seq),
                ));
                push_text(&mut out, Style::new(), "  ", &msg.text);
                out.push((Style::new(), String::new()));
            }
        }
    }
    if !view.stream_thinking.is_empty() {
        push_text(&mut out, dim.italic(), "┆ ", &view.stream_thinking);
    }
    if !view.stream_text.is_empty() {
        out.push((
            Style::new().fg(Color::Magenta).bold(),
            "◆ assistant (streaming)".into(),
        ));
        push_text(
            &mut out,
            Style::new(),
            "  ",
            &format!("{}▌", view.stream_text),
        );
    }
    if view.synced && view.entries.is_empty() {
        out.push((dim, "(nothing yet)".into()));
    }
    out
}

/// Greedy wrap by display width, breaking at the last space when possible.
fn wrap(text: &str, width: usize) -> Vec<String> {
    let text = text.replace('\t', "    ");
    let mut out = Vec::new();
    let mut line = String::new();
    let mut w = 0;
    for c in text.chars() {
        let cw = c.width().unwrap_or(0);
        if w + cw > width && !line.is_empty() {
            match line.rfind(' ').filter(|&i| i > 0) {
                Some(i) => {
                    let rest = line[i + 1..].to_string();
                    line.truncate(i);
                    out.push(std::mem::replace(&mut line, rest));
                    w = line.chars().map(|c| c.width().unwrap_or(0)).sum();
                }
                None => {
                    out.push(std::mem::take(&mut line));
                    w = 0;
                }
            }
        }
        line.push(c);
        w += cw;
    }
    out.push(line);
    out
}

fn draw_input(f: &mut Frame, app: &App, area: Rect) {
    let target = match (app.input.split_whitespace().next(), viewing(app)) {
        (Some(c), _) if c.starts_with('/') => "command".to_string(),
        (Some(t), _) if t.starts_with('@') => format!("DM {t} as @human"),
        (Some(t), _) if t.starts_with('#') && t.len() > 1 => format!("post to {t} as @human"),
        (_, Some(ViewKind::Agent(id))) => {
            format!(
                "DM {} as @human",
                app.agent(id).map_or(id.clone(), |a| short(&a.label))
            )
        }
        (_, Some(ViewKind::Log(log))) if log.starts_with("dm:") => {
            format!(
                "DM in {} as @human",
                app.log(log).map_or(log.clone(), |l| short_title(&l.title))
            )
        }
        (_, Some(ViewKind::Log(log))) => {
            format!(
                "post to {} as @human",
                app.log(log).map_or(log.clone(), |l| short_title(&l.title))
            )
        }
        _ => "@agent text · #topic text · /spawn …".to_string(),
    };
    let focused = app.focus == Focus::Input;
    let inner_w = area.width.saturating_sub(4) as usize;
    let before: String = app.input.chars().take(app.cursor).collect();
    let before_w: usize = before.chars().map(|c| c.width().unwrap_or(0)).sum();
    // Keep the cursor visible by dropping leading chars.
    let skip = before_w.saturating_sub(inner_w.saturating_sub(1));
    let mut shown = String::new();
    let mut dropped = 0;
    for c in app.input.chars() {
        if dropped < skip {
            dropped += c.width().unwrap_or(0);
            continue;
        }
        shown.push(c);
    }
    let p = Paragraph::new(format!("› {shown}")).block(block(format!(" → {target} "), focused));
    f.render_widget(p, area);
    if focused {
        f.set_cursor_position((
            area.x + 3 + (before_w - dropped.min(before_w)) as u16,
            area.y + 1,
        ));
    }
}

fn draw_status(f: &mut Frame, app: &App, area: Rect) {
    let (msg, is_err) = &app.status;
    let inbox = app.inbox();
    let right = format!(
        "{}{} agents · {} · ? help ",
        if inbox > 0 {
            format!("✉ {inbox} for @human · ")
        } else {
            String::new()
        },
        app.snap.agents.len(),
        app.api.url,
    );
    let [l, r] = Layout::horizontal([
        Constraint::Min(10),
        Constraint::Length(right.chars().count() as u16),
    ])
    .areas(area);
    let style = if *is_err {
        Style::new().fg(Color::Red)
    } else {
        Style::new().fg(Color::Gray)
    };
    f.render_widget(
        Paragraph::new(format!(" {}", msg.lines().next().unwrap_or(""))).style(style),
        l,
    );
    let rstyle = if inbox > 0 {
        Style::new().fg(Color::Magenta).bold()
    } else {
        Style::new().fg(Color::DarkGray)
    };
    f.render_widget(Paragraph::new(right).style(rstyle), r);
}

fn draw_help(f: &mut Frame) {
    let text = "\
Keys
  Tab / Shift-Tab   cycle focus: agents · topics & DMs · input
  ↑↓ / j k          select            Enter   watch it live
  PgUp / PgDn       scroll the view   End/G   follow (Home: top)
  s                 /spawn …          t       /topic …
  x                 /kill selected agent       i  type      q  quit
  Esc               leave the input   Ctrl-C  quit

Input (you are @human)
  text              DM the viewed agent / post to the viewed topic or DM
  @agent text       DM an agent        #topic text   post (creates #topic)

Commands
  /spawn [--name N] [--model M] [--thinking L] [--cwd D] [--tools a,b] <task>
  /topic <name> [description]     /invite @a @b    (in a topic view)
  /rename <name>  (topic view)    /kill <agent>    /open @agent|#topic
  /dm @agent      your DM log with an agent       /quit

Press any key to close.";
    let area = f.area();
    let w = 78.min(area.width.saturating_sub(4));
    let h = 25.min(area.height.saturating_sub(2));
    let rect = Rect::new(
        area.x + (area.width - w) / 2,
        area.y + (area.height - h) / 2,
        w,
        h,
    );
    f.render_widget(Clear, rect);
    f.render_widget(
        Paragraph::new(text)
            .block(block(" Help ", true))
            .style(Style::new().add_modifier(Modifier::empty())),
        rect,
    );
}
