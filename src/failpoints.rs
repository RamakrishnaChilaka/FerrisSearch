use anyhow::{Result, bail};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::Notify;

pub const PRIMARY_AFTER_LOCAL_APPLY_BEFORE_REPLICATION: &str =
    "primary_after_local_apply_before_replication";

#[derive(Debug, Default)]
struct PauseState {
    hits: AtomicUsize,
    reached: Notify,
    released: AtomicBool,
    release: Notify,
}

fn registry() -> &'static Mutex<HashMap<&'static str, Arc<PauseState>>> {
    static REGISTRY: OnceLock<Mutex<HashMap<&'static str, Arc<PauseState>>>> = OnceLock::new();
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
        if registry
            .get(self.name)
            .is_some_and(|state| Arc::ptr_eq(state, &self.state))
        {
            registry.remove(self.name);
        }
    }
}

pub fn install_pause(name: &'static str) -> Result<PauseHandle> {
    let state = Arc::new(PauseState::default());
    let mut registry = registry().lock().unwrap_or_else(|error| error.into_inner());
    if registry.insert(name, state.clone()).is_some() {
        bail!("failpoint [{name}] is already installed");
    }
    Ok(PauseHandle { name, state })
}

pub async fn pause(name: &'static str) {
    let state = registry()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(name)
        .cloned();
    let Some(state) = state else {
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
}
