//! Logind session event source for suspend/hibernate handling.
//!
//! This module provides a calloop event source that monitors the logind
//! `PrepareForSleep` D-Bus signal to detect when the system is going to
//! sleep (suspend or hibernate) and when it wakes up.
//!
//! libseat's session events don't fire for suspend/hibernate, only for VT switches.
//! This event source fills that gap by providing [`Event::PreparingSleep`] and
//! [`Event::ResumedFromSleep`] events.
//!
//! ## Usage
//!
//! This event source is designed to be used alongside the libseat session notifier.
//! You can insert both into your calloop event loop:
//!
//! ```ignore
//! use smithay::backend::session::{libseat::LibSeatSession, logind::LogindSessionNotifier, Event};
//!
//! let (session, libseat_notifier) = LibSeatSession::new()?;
//! let logind_notifier = LogindSessionNotifier::new()?;
//!
//! event_loop.handle().insert_source(libseat_notifier, |event, _, state| {
//!     match event {
//!         Event::PauseSession => { /* handle VT switch away */ }
//!         Event::ActivateSession => { /* handle VT switch back */ }
//!         _ => {}
//!     }
//! })?;
//!
//! event_loop.handle().insert_source(logind_notifier, |event, _, state| {
//!     match event {
//!         Event::PreparingSleep => { /* prepare for sleep */ }
//!         Event::ResumedFromSleep => { /* refresh DRM state after wake */ }
//!         _ => {}
//!     }
//! })?;
//! ```

use std::io;
use std::thread;

use calloop::{EventSource, Poll, PostAction, Readiness, Token, TokenFactory};
use tracing::{debug, info_span, warn};

use crate::backend::session::Event as SessionEvent;

/// Event source that monitors logind's `PrepareForSleep` D-Bus signal.
///
/// This provides [`SessionEvent::PreparingSleep`] and [`SessionEvent::ResumedFromSleep`]
/// events to complement libseat's VT switch events.
#[derive(Debug)]
pub struct LogindSessionNotifier {
    rx: calloop::channel::Channel<SessionEvent>,
    /// Handle to the background thread, kept to ensure it stays alive
    _thread_handle: thread::JoinHandle<()>,
    #[allow(dead_code)]
    span: tracing::Span,
}

impl LogindSessionNotifier {
    /// Creates a new logind session notifier.
    ///
    /// This spawns a background thread to handle D-Bus communication with logind.
    /// The thread will monitor the `PrepareForSleep` signal and send events through
    /// a channel that can be polled in your calloop event loop.
    pub fn new() -> Result<Self, Error> {
        let span = info_span!("backend_session", "type" = "logind");
        let _guard = span.enter();

        let (tx, rx) = calloop::channel::channel();

        let thread_handle = thread::Builder::new()
            .name("smithay-logind".into())
            .spawn(move || {
                if let Err(err) = run_logind_monitor(tx) {
                    warn!("logind monitor thread error: {err:?}");
                }
            })
            .map_err(|e| Error::ThreadSpawn(e))?;

        drop(_guard);

        Ok(Self {
            rx,
            _thread_handle: thread_handle,
            span,
        })
    }
}

fn run_logind_monitor(tx: calloop::channel::Sender<SessionEvent>) -> Result<(), Error> {
    // Use blocking zbus API since we're in a dedicated thread
    let conn = zbus::blocking::Connection::system().map_err(Error::DbusConnection)?;

    // Create a proxy for the Manager interface
    let proxy = zbus::blocking::Proxy::new(
        &conn,
        "org.freedesktop.login1",
        "/org/freedesktop/login1",
        "org.freedesktop.login1.Manager",
    )
    .map_err(Error::DbusProxy)?;

    // Subscribe to PrepareForSleep signal
    // The signal has signature "b" (boolean): true = going to sleep, false = waking up
    let mut stream = proxy
        .receive_signal("PrepareForSleep")
        .map_err(Error::DbusSignal)?;

    debug!("logind PrepareForSleep monitor started");

    loop {
        // Block waiting for the next signal
        let signal = match stream.next() {
            Some(s) => s,
            None => {
                debug!("logind signal stream ended");
                break;
            }
        };

        // Parse the boolean argument
        let body = signal.body();
        let start: bool = match body.deserialize() {
            Ok(v) => v,
            Err(err) => {
                warn!("failed to parse PrepareForSleep signal: {err:?}");
                continue;
            }
        };

        let event = if start {
            debug!("PrepareForSleep: going to sleep");
            SessionEvent::PreparingSleep
        } else {
            debug!("PrepareForSleep: waking up");
            SessionEvent::ResumedFromSleep
        };

        if tx.send(event).is_err() {
            // Channel closed, receiver dropped
            debug!("logind event channel closed");
            break;
        }
    }

    Ok(())
}

impl EventSource for LogindSessionNotifier {
    type Event = SessionEvent;
    type Metadata = ();
    type Ret = ();
    type Error = Error;

    fn process_events<F>(
        &mut self,
        readiness: Readiness,
        token: Token,
        mut callback: F,
    ) -> Result<PostAction, Self::Error>
    where
        F: FnMut(Self::Event, &mut Self::Metadata) -> Self::Ret,
    {
        self.rx
            .process_events(readiness, token, |event, _| {
                if let calloop::channel::Event::Msg(session_event) = event {
                    callback(session_event, &mut ());
                }
            })
            .map_err(|_| Error::ChannelClosed)
    }

    fn register(&mut self, poll: &mut Poll, factory: &mut TokenFactory) -> calloop::Result<()> {
        self.rx.register(poll, factory)
    }

    fn reregister(&mut self, poll: &mut Poll, factory: &mut TokenFactory) -> calloop::Result<()> {
        self.rx.reregister(poll, factory)
    }

    fn unregister(&mut self, poll: &mut Poll) -> calloop::Result<()> {
        self.rx.unregister(poll)
    }
}

/// Errors that can occur with the logind session notifier.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Failed to connect to the D-Bus system bus
    #[error("Failed to connect to D-Bus system bus: {0}")]
    DbusConnection(#[source] zbus::Error),

    /// Failed to create D-Bus proxy
    #[error("Failed to create D-Bus proxy: {0}")]
    DbusProxy(#[source] zbus::Error),

    /// Failed to subscribe to D-Bus signal
    #[error("Failed to subscribe to PrepareForSleep signal: {0}")]
    DbusSignal(#[source] zbus::Error),

    /// Failed to spawn background thread
    #[error("Failed to spawn logind monitor thread: {0}")]
    ThreadSpawn(#[source] io::Error),

    /// The event channel was closed unexpectedly
    #[error("Event channel closed")]
    ChannelClosed,
}
