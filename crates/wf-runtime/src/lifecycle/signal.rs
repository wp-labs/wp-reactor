use tokio_util::sync::CancellationToken;

#[derive(::moju_derive::MoJu, Debug, Clone, Copy, PartialEq, Eq)]
#[moju(kind = "state", domain = "Runtime", module = "Runtime.ReactorLifecycle")]
pub enum ShutdownTrigger {
    Signal,
    Internal,
}

/// Register Ctrl-C (SIGINT) and SIGTERM handling; cancel the engine on first
/// signal received.
pub async fn wait_for_signal(cancel: CancellationToken) -> ShutdownTrigger {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = signal(SignalKind::terminate()).expect("failed to listen for SIGTERM");
        tokio::select! {
            _ = cancel.cancelled() => ShutdownTrigger::Internal,
            _ = tokio::signal::ctrl_c() => {
                wf_info!(sys, signal = "SIGINT", "received signal, initiating graceful shutdown");
                cancel.cancel();
                ShutdownTrigger::Signal
            }
            _ = sigterm.recv() => {
                wf_info!(sys, signal = "SIGTERM", "received signal, initiating graceful shutdown");
                cancel.cancel();
                ShutdownTrigger::Signal
            }
        }
    }
    #[cfg(not(unix))]
    {
        tokio::select! {
            _ = cancel.cancelled() => ShutdownTrigger::Internal,
            _ = tokio::signal::ctrl_c() => {
                wf_info!(
                    sys,
                    "received shutdown signal, initiating graceful shutdown"
                );
                cancel.cancel();
                ShutdownTrigger::Signal
            }
        }
    }
}
