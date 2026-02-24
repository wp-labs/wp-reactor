use tokio_util::sync::CancellationToken;

/// Register Ctrl-C (SIGINT) and SIGTERM handling; cancel the engine on first
/// signal received.
pub async fn wait_for_signal(cancel: CancellationToken) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = signal(SignalKind::terminate()).expect("failed to listen for SIGTERM");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                wf_info!(sys, signal = "SIGINT", "received signal, initiating graceful shutdown");
            }
            _ = sigterm.recv() => {
                wf_info!(sys, signal = "SIGTERM", "received signal, initiating graceful shutdown");
            }
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for Ctrl-C");
        wf_info!(
            sys,
            "received shutdown signal, initiating graceful shutdown"
        );
    }
    cancel.cancel();
}
