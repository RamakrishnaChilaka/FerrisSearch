//! Reactive settings manager.
//!
//! Provides a pub/sub layer over the serializable `IndexSettings`.
//! Consumers subscribe to individual settings via `tokio::sync::watch` channels
//! and automatically react when settings change — no polling, no push method
//! calls on engines.
//!
//! ## Architecture
//!
//! ```text
//! PUT /{index}/_settings
//!       │
//!       ▼
//!   Raft commit → state machine apply → ClusterState updated
//!       │
//!       ▼
//!   ShardManager::apply_settings(index, new_settings)
//!       │
//!       ▼
//!   SettingsManager::update(new_values)
//!       │
//!       ├── watch::Sender<Duration>::send(new_refresh_interval)
//!       │         └── refresh loop wakes up, adjusts interval
//!       ├── (future) watch::Sender<TranslogDurability>::send(...)
//!       │         └── WAL switches mode
//!       └── (future) more consumers...
//! ```
//!
//! ## Adding a new reactive setting
//!
//! 1. Add the field to `IndexSettings` in `state.rs`
//! 2. Add a `watch::Sender<T>` + accessor in `SettingsManager`
//! 3. In `SettingsManager::update()`, detect changes and send to the channel
//! 4. In the consumer (engine, WAL, etc.), subscribe via `watch_*()` and
//!    use `tokio::select!` to react when the value changes

use crate::cluster::state::IndexSettings;
use std::time::Duration;
use tokio::sync::watch;

/// Default refresh interval when no per-index override is set.
pub const DEFAULT_REFRESH_INTERVAL_MS: u64 = 5000;

/// Reactive settings manager for a single index.
///
/// Wraps the serializable `IndexSettings` and vends `watch::Receiver<T>`
/// channels so consumers can subscribe and react to setting changes
/// without the settings system knowing about its consumers.
pub struct SettingsManager {
    /// Current settings snapshot.
    values: std::sync::RwLock<IndexSettings>,

    // ── Watch channels (one per reactive setting) ───────────────────

    /// Refresh interval watch channel — consumed by the background refresh loop.
    refresh_interval_tx: watch::Sender<Duration>,
}

impl SettingsManager {
    /// Create a new settings manager with initial values.
    pub fn new(initial: &IndexSettings) -> Self {
        let refresh_ms = initial
            .refresh_interval_ms
            .unwrap_or(DEFAULT_REFRESH_INTERVAL_MS);
        let (tx, _rx) = watch::channel(Duration::from_millis(refresh_ms));

        Self {
            values: std::sync::RwLock::new(initial.clone()),
            refresh_interval_tx: tx,
        }
    }

    /// Subscribe to refresh interval changes.
    /// The returned receiver will yield the current value immediately,
    /// then wake up whenever the value changes.
    pub fn watch_refresh_interval(&self) -> watch::Receiver<Duration> {
        self.refresh_interval_tx.subscribe()
    }

    /// Get the current effective refresh interval.
    pub fn refresh_interval(&self) -> Duration {
        *self.refresh_interval_tx.borrow()
    }

    /// Update settings from new values (e.g. after Raft apply).
    /// Detects which settings actually changed and notifies only those consumers.
    pub fn update(&self, new_values: &IndexSettings) {
        let mut values = self.values.write().unwrap_or_else(|e| e.into_inner());

        // ── Refresh interval ────────────────────────────────────────
        if new_values.refresh_interval_ms != values.refresh_interval_ms {
            let ms = new_values
                .refresh_interval_ms
                .unwrap_or(DEFAULT_REFRESH_INTERVAL_MS);
            let _ = self.refresh_interval_tx.send(Duration::from_millis(ms));
            tracing::info!(
                "Setting refresh_interval_ms updated: {:?} → {}ms",
                values.refresh_interval_ms,
                ms
            );
        }

        // ── Future settings go here ─────────────────────────────────
        // When you add a new reactive setting:
        // 1. Add a watch::Sender<T> field to SettingsManager
        // 2. Add a watch_*() accessor method
        // 3. Detect the change here and send the new value

        *values = new_values.clone();
    }

    /// Get a snapshot of the current settings values.
    pub fn current(&self) -> IndexSettings {
        self.values
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_refresh_interval() {
        let mgr = SettingsManager::new(&IndexSettings::default());
        assert_eq!(
            mgr.refresh_interval(),
            Duration::from_millis(DEFAULT_REFRESH_INTERVAL_MS)
        );
    }

    #[test]
    fn custom_refresh_interval() {
        let settings = IndexSettings {
            refresh_interval_ms: Some(10_000),
        };
        let mgr = SettingsManager::new(&settings);
        assert_eq!(mgr.refresh_interval(), Duration::from_secs(10));
    }

    #[test]
    fn watch_receives_initial_value() {
        let settings = IndexSettings {
            refresh_interval_ms: Some(2000),
        };
        let mgr = SettingsManager::new(&settings);
        let rx = mgr.watch_refresh_interval();
        assert_eq!(*rx.borrow(), Duration::from_millis(2000));
    }

    #[test]
    fn update_notifies_watcher() {
        let mgr = SettingsManager::new(&IndexSettings::default());
        let rx = mgr.watch_refresh_interval();

        // Initial value
        assert_eq!(
            *rx.borrow(),
            Duration::from_millis(DEFAULT_REFRESH_INTERVAL_MS)
        );

        // Update
        mgr.update(&IndexSettings {
            refresh_interval_ms: Some(15_000),
        });
        assert_eq!(*rx.borrow(), Duration::from_millis(15_000));
    }

    #[test]
    fn update_to_none_resets_to_default() {
        let mgr = SettingsManager::new(&IndexSettings {
            refresh_interval_ms: Some(10_000),
        });
        let rx = mgr.watch_refresh_interval();

        mgr.update(&IndexSettings {
            refresh_interval_ms: None,
        });
        assert_eq!(
            *rx.borrow(),
            Duration::from_millis(DEFAULT_REFRESH_INTERVAL_MS)
        );
    }

    #[test]
    fn no_change_does_not_notify() {
        let settings = IndexSettings {
            refresh_interval_ms: Some(5000),
        };
        let mgr = SettingsManager::new(&settings);
        let mut rx = mgr.watch_refresh_interval();

        // Mark initial value as seen
        rx.borrow_and_update();

        // Update with same value — should NOT mark as changed
        mgr.update(&settings);

        // has_changed should be false since value didn't change
        assert!(!rx.has_changed().unwrap());
    }

    #[test]
    fn multiple_watchers_all_notified() {
        let mgr = SettingsManager::new(&IndexSettings::default());
        let rx1 = mgr.watch_refresh_interval();
        let rx2 = mgr.watch_refresh_interval();
        let rx3 = mgr.watch_refresh_interval();

        mgr.update(&IndexSettings {
            refresh_interval_ms: Some(7777),
        });

        assert_eq!(*rx1.borrow(), Duration::from_millis(7777));
        assert_eq!(*rx2.borrow(), Duration::from_millis(7777));
        assert_eq!(*rx3.borrow(), Duration::from_millis(7777));
    }

    #[test]
    fn current_returns_latest_snapshot() {
        let mgr = SettingsManager::new(&IndexSettings::default());
        assert_eq!(mgr.current(), IndexSettings::default());

        let new = IndexSettings {
            refresh_interval_ms: Some(3000),
        };
        mgr.update(&new);
        assert_eq!(mgr.current(), new);
    }

    #[test]
    fn sequential_updates() {
        let mgr = SettingsManager::new(&IndexSettings::default());
        let rx = mgr.watch_refresh_interval();

        mgr.update(&IndexSettings {
            refresh_interval_ms: Some(1000),
        });
        assert_eq!(*rx.borrow(), Duration::from_millis(1000));

        mgr.update(&IndexSettings {
            refresh_interval_ms: Some(2000),
        });
        assert_eq!(*rx.borrow(), Duration::from_millis(2000));

        mgr.update(&IndexSettings {
            refresh_interval_ms: None,
        });
        assert_eq!(
            *rx.borrow(),
            Duration::from_millis(DEFAULT_REFRESH_INTERVAL_MS)
        );
    }
}
