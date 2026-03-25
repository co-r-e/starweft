use std::io;
use std::time::{Duration, Instant};

use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use crossterm::execute;
use crossterm::terminal::{self, EnterAlternateScreen, LeaveAlternateScreen};
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Gauge, Paragraph, Row, Table};
use ratatui::{DefaultTerminal, Frame};

use crate::cli::DashboardArgs;
use crate::status::{StatusView, load_status_view};

pub(crate) fn run_dashboard(args: DashboardArgs) -> Result<()> {
    let interval = Duration::from_millis(args.interval_ms);

    terminal::enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let mut terminal = ratatui::init();

    let result = run_loop(&mut terminal, args.data_dir.as_ref(), interval);

    ratatui::restore();
    execute!(io::stdout(), LeaveAlternateScreen)?;
    terminal::disable_raw_mode()?;

    result
}

fn run_loop(
    terminal: &mut DefaultTerminal,
    data_dir: Option<&std::path::PathBuf>,
    interval: Duration,
) -> Result<()> {
    let mut last_view: Option<StatusView> = None;
    let mut last_error: Option<String> = None;
    let mut last_refresh = Instant::now() - interval;

    loop {
        if last_refresh.elapsed() >= interval {
            match load_status_view(data_dir) {
                Ok(view) => {
                    last_view = Some(view);
                    last_error = None;
                }
                Err(error) => {
                    last_error = Some(format!("{error:#}"));
                }
            }
            last_refresh = Instant::now();
        }

        terminal.draw(|frame| draw(frame, last_view.as_ref(), last_error.as_deref()))?;

        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press
                    && matches!(key.code, KeyCode::Char('q') | KeyCode::Esc)
                {
                    return Ok(());
                }
            }
        }
    }
}

fn draw(frame: &mut Frame, view: Option<&StatusView>, error: Option<&str>) {
    let area = frame.area();

    let chunks = Layout::vertical([
        Constraint::Length(3),
        Constraint::Length(9),
        Constraint::Length(9),
        Constraint::Min(5),
        Constraint::Length(1),
    ])
    .split(area);

    draw_header(frame, chunks[0], view);

    if let Some(err) = error {
        let paragraph = Paragraph::new(err)
            .style(Style::default().fg(Color::Red))
            .block(Block::default().borders(Borders::ALL).title("Error"));
        frame.render_widget(paragraph, chunks[1]);
        return;
    }

    if let Some(view) = view {
        draw_overview(frame, chunks[1], view);
        draw_messaging(frame, chunks[2], view);
        draw_role_detail(frame, chunks[3], view);
    }

    draw_footer(frame, chunks[4]);
}

fn health_color(health: &str) -> Color {
    if health.contains("openclaw_enabled=true") {
        Color::Green
    } else if health.contains("openclaw_enabled=false") {
        Color::Yellow
    } else {
        Color::White
    }
}

fn worker_capacity(active: u64, max: u64) -> f64 {
    if max > 0 {
        (active as f64 / max as f64).min(1.0)
    } else {
        0.0
    }
}

fn capacity_color(capacity: f64) -> Color {
    if capacity >= 1.0 {
        Color::Red
    } else if capacity >= 0.75 {
        Color::Yellow
    } else {
        Color::Green
    }
}

fn dead_letter_color(dead_letter_count: u64) -> Color {
    if dead_letter_count > 0 {
        Color::Red
    } else {
        Color::Green
    }
}

fn draw_header(frame: &mut Frame, area: Rect, view: Option<&StatusView>) {
    let title = match view {
        Some(v) => format!(" Starweft Dashboard — {} ({}) ", v.role, v.node_id),
        None => " Starweft Dashboard — loading... ".to_owned(),
    };
    let health = view.map(|v| v.health_summary.clone()).unwrap_or_default();
    let health_color = health_color(&health);

    let block = Block::default()
        .borders(Borders::ALL)
        .title(title)
        .border_style(Style::default().fg(Color::Cyan));
    let inner = block.inner(area);
    frame.render_widget(block, area);
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled("health: ", Style::default().fg(Color::Gray)),
            Span::styled(health, Style::default().fg(health_color)),
        ])),
        inner,
    );
}

fn kv_row(key: &str, value: impl ToString) -> Row<'static> {
    Row::new(vec![key.to_owned(), value.to_string()])
}

fn draw_overview(frame: &mut Frame, area: Rect, view: &StatusView) {
    let rows = vec![
        kv_row("actor_id", &view.actor_id),
        kv_row("node_id", &view.node_id),
        kv_row("known_peers", view.known_peers),
        kv_row(
            "connected_sessions",
            view.connected_sessions
                .map(|count| count.to_string())
                .unwrap_or_else(|| "n/a".to_owned()),
        ),
        kv_row("peer_trust", &view.peer_trust_summary),
        kv_row(
            "blocked_peers",
            if view.peer_dispatch_blocked_preview.is_empty() {
                "none".to_owned()
            } else {
                view.peer_dispatch_blocked_preview.join(", ")
            },
        ),
        kv_row("active_projects", view.active_projects),
        kv_row("running_tasks", view.running_tasks),
    ];

    let table = Table::new(rows, [Constraint::Length(20), Constraint::Min(30)])
        .block(Block::default().borders(Borders::ALL).title(" Overview "))
        .header(
            Row::new(vec!["Key", "Value"]).style(Style::default().add_modifier(Modifier::BOLD)),
        );
    frame.render_widget(table, area);
}

fn draw_messaging(frame: &mut Frame, area: Rect, view: &StatusView) {
    let cols =
        Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)]).split(area);

    let total_outbox = view.queued_outbox + view.retry_waiting_outbox + view.dead_letter_outbox;
    let outbox_rows = vec![
        kv_row("queued", view.queued_outbox),
        kv_row("retry_waiting", view.retry_waiting_outbox),
        kv_row("dead_letter", view.dead_letter_outbox),
    ];
    let dead_color = dead_letter_color(view.dead_letter_outbox);
    let outbox_table = Table::new(outbox_rows, [Constraint::Length(16), Constraint::Min(10)])
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(" Outbox ({total_outbox}) "))
                .border_style(Style::default().fg(dead_color)),
        );
    frame.render_widget(outbox_table, cols[0]);

    let inbox_rows = vec![
        kv_row("unprocessed", view.inbox_unprocessed),
        kv_row("stop_orders", view.stop_orders),
        kv_row("snapshots", view.snapshots),
        kv_row("evaluations", view.evaluations),
        kv_row("artifacts", view.artifacts),
    ];
    let inbox_table = Table::new(inbox_rows, [Constraint::Length(16), Constraint::Min(10)]).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Inbox / State "),
    );
    frame.render_widget(inbox_table, cols[1]);
}

fn draw_role_detail(frame: &mut Frame, area: Rect, view: &StatusView) {
    match view.role.as_str() {
        "worker" => draw_worker_detail(frame, area, view),
        "owner" => draw_owner_detail(frame, area, view),
        "principal" => draw_principal_detail(frame, area, view),
        _ => {
            let p = Paragraph::new(format!("role: {}", view.role))
                .block(Block::default().borders(Borders::ALL).title(" Role "));
            frame.render_widget(p, area);
        }
    }
}

fn role_table<'a>(title: &'a str, rows: Vec<Row<'a>>) -> Table<'a> {
    Table::new(rows, [Constraint::Length(20), Constraint::Min(10)])
        .block(Block::default().borders(Borders::ALL).title(title))
}

fn draw_worker_detail(frame: &mut Frame, area: Rect, view: &StatusView) {
    let cols =
        Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)]).split(area);

    let rows = vec![
        kv_row("assigned_tasks", view.assigned_tasks),
        kv_row("active_assigned", view.active_assigned_tasks),
        kv_row("max_active", view.worker_max_active_tasks),
        kv_row("accept_joins", view.worker_accept_join_offers),
        kv_row("openclaw", view.openclaw_enabled),
    ];
    frame.render_widget(role_table(" Worker ", rows), cols[0]);

    let capacity = worker_capacity(view.active_assigned_tasks, view.worker_max_active_tasks);
    let gauge_color = capacity_color(capacity);
    let gauge = Gauge::default()
        .block(Block::default().borders(Borders::ALL).title(" Capacity "))
        .gauge_style(Style::default().fg(gauge_color))
        .percent((capacity * 100.0) as u16)
        .label(format!(
            "{}/{}",
            view.active_assigned_tasks, view.worker_max_active_tasks
        ));
    frame.render_widget(gauge, cols[1]);
}

fn draw_owner_detail(frame: &mut Frame, area: Rect, view: &StatusView) {
    let rows = vec![
        kv_row("owned_projects", view.owned_projects),
        kv_row("issued_tasks", view.issued_tasks),
        kv_row("max_retry", view.owner_max_retry_attempts),
        kv_row("retry_cooldown_ms", view.owner_retry_cooldown_ms),
        kv_row(
            "blocked_reason",
            view.latest_project_blocked_reason
                .clone()
                .unwrap_or_else(|| "none".to_owned()),
        ),
    ];
    frame.render_widget(role_table(" Owner ", rows), area);
}

fn draw_principal_detail(frame: &mut Frame, area: Rect, view: &StatusView) {
    let rows = vec![
        kv_row("visions", view.principal_visions),
        kv_row("projects", view.principal_projects),
        kv_row("stop_receipts", view.stop_receipts),
    ];
    frame.render_widget(role_table(" Principal ", rows), area);
}

fn draw_footer(frame: &mut Frame, area: Rect) {
    let footer = Paragraph::new(Line::from(vec![
        Span::styled(
            " q",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("/"),
        Span::styled(
            "Esc",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" 終了"),
    ]));
    frame.render_widget(footer, area);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_color_green_when_openclaw_enabled() {
        assert_eq!(health_color("openclaw_enabled=true peers=2"), Color::Green);
    }

    #[test]
    fn health_color_yellow_when_openclaw_disabled() {
        assert_eq!(
            health_color("openclaw_enabled=false peers=0"),
            Color::Yellow
        );
    }

    #[test]
    fn health_color_white_for_unknown_health() {
        assert_eq!(health_color("ready"), Color::White);
        assert_eq!(health_color(""), Color::White);
    }

    #[test]
    fn worker_capacity_calculates_ratio() {
        assert!((worker_capacity(3, 4) - 0.75).abs() < f64::EPSILON);
        assert!((worker_capacity(0, 4) - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn worker_capacity_clamps_to_one() {
        assert!((worker_capacity(5, 4) - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn worker_capacity_zero_when_max_is_zero() {
        assert!((worker_capacity(0, 0) - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn capacity_color_green_under_75_percent() {
        assert_eq!(capacity_color(0.5), Color::Green);
        assert_eq!(capacity_color(0.0), Color::Green);
    }

    #[test]
    fn capacity_color_yellow_at_75_percent() {
        assert_eq!(capacity_color(0.75), Color::Yellow);
        assert_eq!(capacity_color(0.99), Color::Yellow);
    }

    #[test]
    fn capacity_color_red_at_full() {
        assert_eq!(capacity_color(1.0), Color::Red);
    }

    #[test]
    fn dead_letter_color_red_when_nonzero() {
        assert_eq!(dead_letter_color(1), Color::Red);
        assert_eq!(dead_letter_color(100), Color::Red);
    }

    #[test]
    fn dead_letter_color_green_when_zero() {
        assert_eq!(dead_letter_color(0), Color::Green);
    }
}
