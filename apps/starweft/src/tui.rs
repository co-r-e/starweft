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
        Constraint::Length(7),
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

fn draw_header(frame: &mut Frame, area: Rect, view: Option<&StatusView>) {
    let title = match view {
        Some(v) => format!(" Starweft Dashboard — {} ({}) ", v.role, v.node_id),
        None => " Starweft Dashboard — loading... ".to_owned(),
    };
    let health = view.map(|v| v.health_summary.clone()).unwrap_or_default();
    let health_color = if health.contains("openclaw_enabled=true") {
        Color::Green
    } else if health.contains("openclaw_enabled=false") {
        Color::Yellow
    } else {
        Color::White
    };

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
        kv_row("connected_peers", view.connected_peers),
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
    let dead_color = if view.dead_letter_outbox > 0 {
        Color::Red
    } else {
        Color::Green
    };
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
    let table = Table::new(rows, [Constraint::Length(18), Constraint::Min(10)])
        .block(Block::default().borders(Borders::ALL).title(" Worker "));
    frame.render_widget(table, cols[0]);

    let capacity = if view.worker_max_active_tasks > 0 {
        (view.active_assigned_tasks as f64 / view.worker_max_active_tasks as f64).min(1.0)
    } else {
        0.0
    };
    let gauge_color = if capacity >= 1.0 {
        Color::Red
    } else if capacity >= 0.75 {
        Color::Yellow
    } else {
        Color::Green
    };
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
    ];
    let table = Table::new(rows, [Constraint::Length(20), Constraint::Min(10)])
        .block(Block::default().borders(Borders::ALL).title(" Owner "));
    frame.render_widget(table, area);
}

fn draw_principal_detail(frame: &mut Frame, area: Rect, view: &StatusView) {
    let rows = vec![
        kv_row("visions", view.principal_visions),
        kv_row("projects", view.principal_projects),
        kv_row("stop_receipts", view.stop_receipts),
    ];
    let table = Table::new(rows, [Constraint::Length(20), Constraint::Min(10)])
        .block(Block::default().borders(Borders::ALL).title(" Principal "));
    frame.render_widget(table, area);
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
