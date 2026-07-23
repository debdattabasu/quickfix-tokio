//! End-to-end session-schedule behavior over loopback. Schedules use the
//! real wall clock, so these build windows relative to `now`.

use std::sync::Arc;
use std::time::Duration;

use chrono::{Duration as ChronoDuration, Timelike, Utc};
use quickfix_tokio::{Application, Engine, MemoryStoreFactory, NullLogFactory, Settings};

#[derive(Default)]
struct App;
impl Application for App {}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn hms(t: chrono::DateTime<Utc>) -> String {
    t.format("%H:%M:%S").to_string()
}

async fn wait_logged_on(h: &quickfix_tokio::SessionHandle, secs: u64) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    while tokio::time::Instant::now() < deadline {
        if h.is_logged_on().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

/// A session logs on while inside its window, then logs out on its own when
/// the window closes.
#[tokio::test]
async fn scheduled_logout_when_window_closes() {
    let now = Utc::now();
    let sod = now.time().num_seconds_from_midnight();
    // Avoid the midnight wrap so `start < end` stays a plain daily window.
    if !(20..86_350).contains(&sod) {
        eprintln!("skipping scheduled_logout: too close to midnight");
        return;
    }
    let start = hms(now - ChronoDuration::seconds(15));
    let end = hms(now + ChronoDuration::seconds(3));
    let port = free_port();

    let server = Engine::start(
        &Settings::parse(&format!(
            "[SESSION]\nConnectionType=acceptor\nBeginString=FIX.4.4\nSenderCompID=SERVER\n\
             TargetCompID=CLIENT\nSocketAcceptPort={port}\nHeartBtInt=30\n\
             StartTime={start}\nEndTime={end}\n"
        ))
        .unwrap(),
        Arc::new(App),
        Arc::new(MemoryStoreFactory::new()),
        Arc::new(NullLogFactory),
    )
    .await
    .unwrap();
    let client = Engine::start(
        &Settings::parse(&format!(
            "[SESSION]\nConnectionType=initiator\nBeginString=FIX.4.4\nSenderCompID=CLIENT\n\
             TargetCompID=SERVER\nSocketConnectHost=127.0.0.1\nSocketConnectPort={port}\n\
             HeartBtInt=30\nReconnectInterval=1\nStartTime={start}\nEndTime={end}\n"
        ))
        .unwrap(),
        Arc::new(App),
        Arc::new(MemoryStoreFactory::new()),
        Arc::new(NullLogFactory),
    )
    .await
    .unwrap();

    let cs = client.session("FIX.4.4", "CLIENT", "SERVER").unwrap();
    let ss = server.session("FIX.4.4", "SERVER", "CLIENT").unwrap();
    assert!(wait_logged_on(&cs, 5).await, "did not log on inside the window");
    assert!(ss.is_logged_on().await, "server did not log on inside the window");

    // Wait past EndTime; the scheduled logout should fire on both sides.
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(!cs.is_logged_on().await, "client still logged on after window closed");
    assert!(!ss.is_logged_on().await, "server still logged on after window closed");

    client.stop().await;
    server.stop().await;
}

/// A session configured with a window entirely in the past never logs on.
#[tokio::test]
async fn no_logon_outside_session_time() {
    let sod = Utc::now().time().num_seconds_from_midnight();
    if sod < 10 {
        eprintln!("skipping no_logon_outside_session_time: too close to midnight");
        return;
    }
    let port = free_port();
    // 00:00:01–00:00:02 is in the past for any time-of-day past the first
    // couple of seconds after midnight.
    let schedule = "StartTime=00:00:01\nEndTime=00:00:02\n";

    let server = Engine::start(
        &Settings::parse(&format!(
            "[SESSION]\nConnectionType=acceptor\nBeginString=FIX.4.4\nSenderCompID=SERVER\n\
             TargetCompID=CLIENT\nSocketAcceptPort={port}\nHeartBtInt=30\n{schedule}"
        ))
        .unwrap(),
        Arc::new(App),
        Arc::new(MemoryStoreFactory::new()),
        Arc::new(NullLogFactory),
    )
    .await
    .unwrap();
    let client = Engine::start(
        &Settings::parse(&format!(
            "[SESSION]\nConnectionType=initiator\nBeginString=FIX.4.4\nSenderCompID=CLIENT\n\
             TargetCompID=SERVER\nSocketConnectHost=127.0.0.1\nSocketConnectPort={port}\n\
             HeartBtInt=30\nReconnectInterval=1\n{schedule}"
        ))
        .unwrap(),
        Arc::new(App),
        Arc::new(MemoryStoreFactory::new()),
        Arc::new(NullLogFactory),
    )
    .await
    .unwrap();

    let cs = client.session("FIX.4.4", "CLIENT", "SERVER").unwrap();
    assert!(
        !wait_logged_on(&cs, 3).await,
        "logged on despite being outside the session window"
    );

    client.stop().await;
    server.stop().await;
}
