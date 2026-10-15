use crate::cluster::state::WriteAuthorityIdentity;
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

pub const SCHEMA: &str = "ferrissearch.write-authority.trace/v1";

pub struct Config {
    pub output: PathBuf,
    pub run_id: String,
    pub index_uuid: String,
    pub shard: u32,
    pub nodes: Vec<String>,
}

struct State {
    config: Config,
    events: Vec<Value>,
}

static STATE: OnceLock<Mutex<Option<State>>> = OnceLock::new();

fn state() -> &'static Mutex<Option<State>> {
    STATE.get_or_init(|| Mutex::new(None))
}

pub struct Session {
    run_id: String,
    finished: bool,
}

/// Collect externally serialized scenarios, not a global ordering of concurrent observations.
pub fn start_serialized_witness(config: Config) -> Result<Session> {
    if config.run_id.is_empty() || config.index_uuid.is_empty() || config.nodes.is_empty() {
        bail!("write-authority trace requires an exact run, index UUID, and node set");
    }
    let mut guard = state().lock().unwrap_or_else(|error| error.into_inner());
    if guard.is_some() {
        bail!("a write-authority trace session is already active");
    }
    let run_id = config.run_id.clone();
    *guard = Some(State {
        config,
        events: Vec::new(),
    });
    Ok(Session {
        run_id,
        finished: false,
    })
}

impl Session {
    pub fn finish(mut self) -> Result<PathBuf> {
        let mut guard = state().lock().unwrap_or_else(|error| error.into_inner());
        let state = guard
            .take()
            .context("write-authority trace is not active")?;
        if state.config.run_id != self.run_id {
            bail!("write-authority trace session changed before finish");
        }
        let output = state.config.output;
        super::write_json_file(
            &output,
            &json!({
                "schema": SCHEMA,
                "profile": "serialized-authority-witness",
                "run_id": self.run_id,
                "index_uuid": state.config.index_uuid,
                "shard": state.config.shard,
                "nodes": state.config.nodes,
                "events": state.events,
            }),
        )?;
        self.finished = true;
        Ok(output)
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        if !self.finished {
            let mut guard = state().lock().unwrap_or_else(|error| error.into_inner());
            if guard
                .as_ref()
                .is_some_and(|state| state.config.run_id == self.run_id)
            {
                *guard = None;
            }
        }
    }
}

pub(crate) fn record(event: &str, authority: &WriteAuthorityIdentity, details: Value) {
    let mut guard = state().lock().unwrap_or_else(|error| error.into_inner());
    let Some(state) = guard.as_mut() else {
        return;
    };
    if state.config.index_uuid != authority.index_uuid || state.config.shard != authority.shard_id {
        return;
    }
    state.events.push(json!({
        "step": state.events.len(),
        "event": event,
        "authority": authority,
        "details": details,
    }));
}
