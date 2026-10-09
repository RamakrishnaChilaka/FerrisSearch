use anyhow::{Result, bail};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::Notify;

pub const PRIMARY_AFTER_LOCAL_APPLY_BEFORE_REPLICATION: &str =
    "primary_after_local_apply_before_replication";
pub const REPLICA_AFTER_FENCE_BEFORE_LOCAL_APPLY: &str = "replica_after_fence_before_local_apply";

#[derive(Debug, Default)]
struct PauseState {
    hits: AtomicUsize,
    reached: Notify,
    released: AtomicBool,
    release: Notify,
}

#[derive(Debug)]
struct FailState {
    remaining: AtomicUsize,
    hits: AtomicUsize,
    message: &'static str,
}

#[derive(Clone, Debug)]
enum Control {
    Pause(Arc<PauseState>),
    Fail(Arc<FailState>),
}

fn registry() -> &'static Mutex<HashMap<&'static str, Control>> {
    static REGISTRY: OnceLock<Mutex<HashMap<&'static str, Control>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

#[derive(Debug)]
pub struct PauseHandle {
    name: &'static str,
    state: Arc<PauseState>,
}

impl PauseHandle {
    pub async fn wait_until_hit(&self) {
        loop {
            let reached = self.state.reached.notified();
            if self.hit_count() > 0 {
                return;
            }
            reached.await;
        }
    }

    pub fn hit_count(&self) -> usize {
        self.state.hits.load(Ordering::Acquire)
    }

    pub fn release(&self) {
        self.state.released.store(true, Ordering::Release);
        self.state.release.notify_waiters();
    }
}

impl Drop for PauseHandle {
    fn drop(&mut self) {
        self.release();
        let mut registry = registry().lock().unwrap_or_else(|error| error.into_inner());
        if registry.get(self.name).is_some_and(
            |control| matches!(control, Control::Pause(state) if Arc::ptr_eq(state, &self.state)),
        ) {
            registry.remove(self.name);
        }
    }
}

pub fn install_pause(name: &'static str) -> Result<PauseHandle> {
    let state = Arc::new(PauseState::default());
    let mut registry = registry().lock().unwrap_or_else(|error| error.into_inner());
    match registry.entry(name) {
        Entry::Vacant(entry) => {
            entry.insert(Control::Pause(state.clone()));
        }
        Entry::Occupied(_) => bail!("failpoint [{name}] is already installed"),
    }
    Ok(PauseHandle { name, state })
}

pub async fn pause(name: &'static str) {
    let control = registry()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(name)
        .cloned();
    let Some(Control::Pause(state)) = control else {
        return;
    };

    state.hits.fetch_add(1, Ordering::AcqRel);
    state.reached.notify_waiters();
    loop {
        let released = state.release.notified();
        if state.released.load(Ordering::Acquire) {
            return;
        }
        released.await;
    }
}

#[derive(Debug)]
pub struct FailHandle {
    name: &'static str,
    state: Arc<FailState>,
}

impl FailHandle {
    pub fn hit_count(&self) -> usize {
        self.state.hits.load(Ordering::Acquire)
    }

    pub fn remaining(&self) -> usize {
        self.state.remaining.load(Ordering::Acquire)
    }
}

impl Drop for FailHandle {
    fn drop(&mut self) {
        let mut registry = registry().lock().unwrap_or_else(|error| error.into_inner());
        if registry.get(self.name).is_some_and(
            |control| matches!(control, Control::Fail(state) if Arc::ptr_eq(state, &self.state)),
        ) {
            registry.remove(self.name);
        }
    }
}

pub fn install_fail(
    name: &'static str,
    attempts: usize,
    message: &'static str,
) -> Result<FailHandle> {
    if attempts == 0 {
        bail!("failpoint [{name}] must fail at least one attempt");
    }
    let state = Arc::new(FailState {
        remaining: AtomicUsize::new(attempts),
        hits: AtomicUsize::new(0),
        message,
    });
    let mut registry = registry().lock().unwrap_or_else(|error| error.into_inner());
    match registry.entry(name) {
        Entry::Vacant(entry) => {
            entry.insert(Control::Fail(state.clone()));
        }
        Entry::Occupied(_) => bail!("failpoint [{name}] is already installed"),
    }
    Ok(FailHandle { name, state })
}

pub fn fail(name: &'static str) -> Result<()> {
    let control = registry()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(name)
        .cloned();
    let Some(Control::Fail(state)) = control else {
        return Ok(());
    };

    let mut remaining = state.remaining.load(Ordering::Acquire);
    loop {
        if remaining == 0 {
            return Ok(());
        }
        match state.remaining.compare_exchange_weak(
            remaining,
            remaining - 1,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                state.hits.fetch_add(1, Ordering::AcqRel);
                bail!("injected failpoint [{name}]: {}", state.message);
            }
            Err(observed) => remaining = observed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn named_pause_waits_for_release() {
        const NAME: &str = "named_pause_waits_for_release";
        let handle = install_pause(NAME).unwrap();
        let paused = tokio::spawn(pause(NAME));

        tokio::time::timeout(Duration::from_secs(1), handle.wait_until_hit())
            .await
            .unwrap();
        assert_eq!(handle.hit_count(), 1);
        assert!(!paused.is_finished());

        handle.release();
        tokio::time::timeout(Duration::from_secs(1), paused)
            .await
            .unwrap()
            .unwrap();
    }

    #[test]
    fn named_fail_is_bounded_and_unregisters_on_drop() {
        const NAME: &str = "named_fail_is_bounded_and_unregisters_on_drop";
        let handle = install_fail(NAME, 1, "expected failure").unwrap();
        let error = fail(NAME).unwrap_err();
        assert!(error.to_string().contains("expected failure"));
        assert_eq!(handle.hit_count(), 1);
        assert_eq!(handle.remaining(), 0);
        fail(NAME).unwrap();
        drop(handle);
        fail(NAME).unwrap();
    }

    #[test]
    fn duplicate_install_preserves_the_original_control() {
        const NAME: &str = "duplicate_install_preserves_the_original_control";
        let handle = install_fail(NAME, 1, "original failure").unwrap();
        assert!(install_pause(NAME).is_err());
        assert!(install_fail(NAME, 1, "replacement failure").is_err());
        let error = fail(NAME).unwrap_err();
        assert!(error.to_string().contains("original failure"));
        assert_eq!(handle.hit_count(), 1);
    }
}
