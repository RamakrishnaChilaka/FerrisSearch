use super::parser::BulkDecoder;
use super::{BulkDoc, BulkTargetKey, validate_bulk_document};
use axum::body::{Body, Bytes};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

pub(super) const IO_BYTES: usize = 64 * 1024;
pub(super) const BATCH_ITEMS: usize = 256;
pub(super) const BATCH_BYTES: u64 = 4 * 1024 * 1024;
pub(super) const RPC_ITEMS: usize = 64;
pub(super) const MAX_INDICES: usize = 128;
pub(super) const MAX_TARGETS: usize = 4096;
const MAX_RESULT_BYTES: usize = 128 * 1024 * 1024;

#[derive(Debug)]
pub struct BulkRuntime {
    permits: Arc<Semaphore>,
    max_request_bytes: u64,
    max_spool_bytes: u64,
    idle_timeout: Duration,
    #[cfg(test)]
    reserved_for_test: Arc<AtomicU64>,
}

impl Default for BulkRuntime {
    fn default() -> Self {
        Self::new(&crate::config::AppConfig::default()).expect("valid default bulk limits")
    }
}

impl BulkRuntime {
    pub fn new(config: &crate::config::AppConfig) -> Result<Self, String> {
        config.validate_operational_limits()?;
        Ok(Self {
            permits: Arc::new(Semaphore::new(config.bulk_max_concurrent_requests)),
            max_request_bytes: config.bulk_max_request_bytes,
            max_spool_bytes: config.bulk_max_spool_bytes,
            idle_timeout: Duration::from_millis(config.bulk_idle_timeout_ms),
            #[cfg(test)]
            reserved_for_test: Arc::new(AtomicU64::new(0)),
        })
    }

    pub(super) fn acquire(&self) -> Result<Arc<RequestLease>, BulkError> {
        let permit = self.permits.clone().try_acquire_owned().map_err(|error| {
            crate::metrics::BULK_ADMISSION_REJECTIONS_TOTAL.inc();
            BulkError::Admission(format!("Bulk request admission exhausted: {error}"))
        })?;
        crate::metrics::BULK_ACTIVE_REQUESTS.inc();
        Ok(Arc::new(RequestLease {
            _permit: permit,
            reserved: AtomicU64::new(0),
            maximum: self.max_spool_bytes,
            terminal: AtomicU8::new(0),
            cancelled: AtomicBool::new(false),
            #[cfg(test)]
            runtime_reserved_for_test: self.reserved_for_test.clone(),
        }))
    }

    #[cfg(all(test, feature = "protocol-trace"))]
    pub(crate) fn reserved_spool_bytes_for_test(&self) -> u64 {
        self.reserved_for_test.load(Ordering::Acquire)
    }

    #[cfg(all(test, feature = "protocol-trace"))]
    pub(crate) fn available_permits_for_test(&self) -> usize {
        self.permits.available_permits()
    }
}

pub(super) struct RequestLease {
    _permit: OwnedSemaphorePermit,
    reserved: AtomicU64,
    maximum: u64,
    terminal: AtomicU8,
    cancelled: AtomicBool,
    #[cfg(test)]
    runtime_reserved_for_test: Arc<AtomicU64>,
}

impl RequestLease {
    pub fn cancellation_flag(&self) -> &AtomicBool {
        &self.cancelled
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    fn check_cancelled(&self) -> io::Result<()> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "Bulk request cancelled",
            ));
        }
        Ok(())
    }

    fn reserve(&self, bytes: usize) -> io::Result<()> {
        self.reserved
            .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current
                    .checked_add(bytes as u64)
                    .filter(|next| *next <= self.maximum)
            })
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::StorageFull,
                    format!(
                        "Bulk input/result spool exceeds {} reserved bytes",
                        self.maximum
                    ),
                )
            })?;
        crate::metrics::BULK_SPOOL_RESERVED_BYTES.add(bytes as i64);
        #[cfg(test)]
        self.runtime_reserved_for_test
            .fetch_add(bytes as u64, Ordering::AcqRel);
        Ok(())
    }

    fn release(&self, bytes: u64) {
        self.reserved.fetch_sub(bytes, Ordering::AcqRel);
        crate::metrics::BULK_SPOOL_RESERVED_BYTES.sub(bytes as i64);
        #[cfg(test)]
        self.runtime_reserved_for_test
            .fetch_sub(bytes, Ordering::AcqRel);
    }

    pub fn fail(&self) {
        self.terminal.store(1, Ordering::Release);
    }

    pub fn complete(&self) {
        self.terminal.store(2, Ordering::Release);
    }
}

pub(super) struct RequestScope {
    lease: Arc<RequestLease>,
    armed: bool,
}

impl RequestScope {
    pub fn new(lease: Arc<RequestLease>) -> Self {
        Self { lease, armed: true }
    }

    pub fn handoff(&mut self) {
        self.armed = false;
    }
}

impl Drop for RequestScope {
    fn drop(&mut self) {
        if self.armed {
            self.lease.cancelled.store(true, Ordering::Release);
        }
    }
}

impl Drop for RequestLease {
    fn drop(&mut self) {
        if self.terminal.load(Ordering::Acquire) == 0 {
            crate::metrics::BULK_CANCELLED_REQUESTS_TOTAL.inc();
        }
        crate::metrics::BULK_ACTIVE_REQUESTS.dec();
    }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum BulkError {
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Limit(String),
    #[error("{0}")]
    Admission(String),
    #[error("Bulk I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("Bulk blocking worker failed: {0}")]
    Worker(#[from] tokio::task::JoinError),
    #[error("Bulk request body made no progress before its idle deadline")]
    Timeout,
}

impl BulkError {
    pub fn response(&self, dispatched: bool, completed_items: u64) -> Response {
        let (status, kind) = if dispatched {
            (StatusCode::INTERNAL_SERVER_ERROR, "write_outcome_unknown")
        } else {
            match self {
                Self::Invalid(_) => (StatusCode::BAD_REQUEST, "illegal_argument_exception"),
                Self::Limit(_) => (StatusCode::PAYLOAD_TOO_LARGE, "content_too_long_exception"),
                Self::Admission(_) => (
                    StatusCode::TOO_MANY_REQUESTS,
                    "es_rejected_execution_exception",
                ),
                Self::Timeout => (StatusCode::REQUEST_TIMEOUT, "bulk_request_timeout"),
                Self::Io(error) if error.kind() == io::ErrorKind::InvalidData => {
                    (StatusCode::BAD_REQUEST, "illegal_argument_exception")
                }
                Self::Io(error) if error.kind() == io::ErrorKind::FileTooLarge => {
                    (StatusCode::PAYLOAD_TOO_LARGE, "content_too_long_exception")
                }
                Self::Io(error) if error.kind() == io::ErrorKind::StorageFull => {
                    (StatusCode::INSUFFICIENT_STORAGE, "bulk_spool_exception")
                }
                _ => (StatusCode::INTERNAL_SERVER_ERROR, "bulk_spool_exception"),
            }
        };
        let reason = if dispatched {
            format!(
                "{self}; bulk mutations may have executed; {completed_items} earlier-batch item \
                 results were stored and the current batch may be partial; retry requires the \
                 original operation IDs"
            )
        } else {
            self.to_string()
        };
        crate::api::error_response(status, kind, reason).into_response()
    }
}

pub(super) struct Spool {
    io: Option<SpoolIo>,
    reserved: u64,
    lease: Arc<RequestLease>,
    runtime: tokio::runtime::Handle,
    nonce: [u8; 16],
    written_frames: u64,
    read_frames: u64,
}

enum SpoolIo {
    Writing(BufWriter<File>),
    Reading(BufReader<File>),
}

impl Spool {
    pub fn new(directory: &std::path::Path, lease: Arc<RequestLease>) -> io::Result<Self> {
        let file = tempfile::tempfile_in(directory)?;
        Ok(Self {
            io: Some(SpoolIo::Writing(BufWriter::with_capacity(IO_BYTES, file))),
            reserved: 0,
            lease,
            runtime: tokio::runtime::Handle::current(),
            nonce: uuid::Uuid::new_v4().into_bytes(),
            written_frames: 0,
            read_frames: 0,
        })
    }

    pub fn rewind(&mut self) -> io::Result<()> {
        self.flush()?;
        let mut reader = match self.io.take().expect("open spool") {
            SpoolIo::Writing(writer) => match writer.into_inner() {
                Ok(file) => BufReader::with_capacity(IO_BYTES, file),
                Err(error) => {
                    let cause = io::Error::new(error.error().kind(), error.error().to_string());
                    self.io = Some(SpoolIo::Writing(error.into_inner()));
                    return Err(cause);
                }
            },
            SpoolIo::Reading(reader) => reader,
        };
        let result = reader.seek(SeekFrom::Start(0));
        self.io = Some(SpoolIo::Reading(reader));
        self.read_frames = 0;
        result.map(|_| ())
    }

    fn digest(&self, counter: u64, size: u32, bytes: &[u8]) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(self.nonce);
        digest.update(counter.to_le_bytes());
        digest.update(size.to_le_bytes());
        digest.update(bytes);
        digest.finalize().into()
    }

    fn append_frame(&mut self, bytes: &[u8]) -> io::Result<()> {
        let size = u32::try_from(bytes.len())
            .map_err(|_| io::Error::other("Bulk spool frame length exceeds u32"))?;
        let counter = self.written_frames;
        let digest = self.digest(counter, size, bytes);
        self.write_all(&size.to_le_bytes())?;
        self.write_all(&counter.to_le_bytes())?;
        self.write_all(&digest)?;
        self.write_all(bytes)?;
        self.written_frames += 1;
        Ok(())
    }

    fn read_frame(&mut self, maximum: usize) -> io::Result<Option<Vec<u8>>> {
        let mut length = [0; 4];
        if self.read(&mut length[..1])? == 0 {
            if self.read_frames != self.written_frames {
                return Err(io::Error::other(format!(
                    "Bulk spool truncated: read {} of {} protected frames",
                    self.read_frames, self.written_frames
                )));
            }
            return Ok(None);
        }
        self.read_exact(&mut length[1..])?;
        let size = u32::from_le_bytes(length) as usize;
        if size == 0 || size > maximum || self.read_frames >= self.written_frames {
            return Err(io::Error::other("Invalid bulk spool frame length or count"));
        }
        let mut counter = [0; 8];
        let mut expected = [0; 32];
        self.read_exact(&mut counter)?;
        self.read_exact(&mut expected)?;
        let counter = u64::from_le_bytes(counter);
        if counter != self.read_frames {
            return Err(io::Error::other("Bulk spool frame order changed"));
        }
        let mut bytes = vec![0; size];
        self.read_exact(&mut bytes)?;
        if self.digest(counter, size as u32, &bytes) != expected {
            return Err(io::Error::other("Bulk spool frame checksum mismatch"));
        }
        self.read_frames += 1;
        Ok(Some(bytes))
    }

    pub fn append_result(&mut self, result: &StoredResult) -> io::Result<()> {
        let mut writer = LimitedResultWriter { bytes: Vec::new() };
        serde_json::to_writer(&mut writer, result)?;
        self.append_frame(&writer.bytes)
    }
}

impl Write for Spool {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.lease.check_cancelled()?;
        self.lease.reserve(bytes.len())?;
        self.reserved += bytes.len() as u64;
        match self.io.as_mut().expect("open spool") {
            SpoolIo::Writing(writer) => writer.write_all(bytes)?,
            SpoolIo::Reading(_) => {
                return Err(io::Error::other("Cannot append to a sealed bulk spool"));
            }
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        match self.io.as_mut().expect("open spool") {
            SpoolIo::Writing(writer) => writer.flush(),
            SpoolIo::Reading(_) => Ok(()),
        }
    }
}

impl Read for Spool {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.lease.check_cancelled()?;
        match self.io.as_mut().expect("open spool") {
            SpoolIo::Reading(reader) => reader.read(bytes),
            SpoolIo::Writing(_) => Err(io::Error::other("Cannot read an unsealed bulk spool")),
        }
    }
}

impl BufRead for Spool {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.lease.check_cancelled()?;
        match self.io.as_mut().expect("open spool") {
            SpoolIo::Reading(reader) => reader.fill_buf(),
            SpoolIo::Writing(_) => Err(io::Error::other("Cannot read an unsealed bulk spool")),
        }
    }

    fn consume(&mut self, bytes: usize) {
        match self.io.as_mut().expect("open spool") {
            SpoolIo::Reading(reader) => reader.consume(bytes),
            SpoolIo::Writing(_) => panic!("Cannot consume an unsealed bulk spool"),
        }
    }
}

impl Drop for Spool {
    fn drop(&mut self) {
        if let Some(mut file) = self.io.take() {
            let lease = self.lease.clone();
            let reserved = self.reserved;
            self.runtime.spawn_blocking(move || {
                if let SpoolIo::Writing(writer) = &mut file
                    && let Err(error) = writer.flush()
                {
                    tracing::error!(error = %error, "Closing bulk spool after I/O failure");
                }
                drop(file);
                lease.release(reserved);
            });
        }
    }
}

struct LimitedResultWriter {
    bytes: Vec<u8>,
}

impl Write for LimitedResultWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_RESULT_BYTES - self.bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::FileTooLarge,
                "Bulk item result exceeds its 128 MiB spool record limit",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(super) struct VerifiedInput {
    spool: Spool,
    bytes: Vec<u8>,
    cursor: usize,
}

impl Read for VerifiedInput {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let count = self.fill_buf()?.len().min(output.len());
        output[..count].copy_from_slice(&self.bytes[self.cursor..self.cursor + count]);
        self.consume(count);
        Ok(count)
    }
}

impl BufRead for VerifiedInput {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        if self.cursor == self.bytes.len() {
            self.bytes = self.spool.read_frame(IO_BYTES)?.unwrap_or_default();
            self.cursor = 0;
        }
        Ok(&self.bytes[self.cursor..])
    }

    fn consume(&mut self, bytes: usize) {
        self.cursor += bytes;
    }
}

enum Input {
    Data(Bytes),
    End,
}

struct InputReader {
    receiver: mpsc::Receiver<Input>,
    current: Bytes,
    spool: Spool,
    input_bytes: u64,
    maximum: u64,
    utf8: Vec<u8>,
    ended: bool,
}

impl Read for InputReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        while self.current.is_empty() {
            if self.ended {
                return Ok(0);
            }
            match self.receiver.blocking_recv() {
                Some(Input::Data(bytes)) => {
                    self.input_bytes = self
                        .input_bytes
                        .checked_add(bytes.len() as u64)
                        .filter(|size| *size <= self.maximum)
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::FileTooLarge,
                                format!("Bulk body exceeds {} bytes", self.maximum),
                            )
                        })?;
                    self.utf8.extend_from_slice(&bytes);
                    let incomplete = match std::str::from_utf8(&self.utf8) {
                        Ok(_) => 0,
                        Err(error) if error.error_len().is_none() => {
                            self.utf8.len() - error.valid_up_to()
                        }
                        Err(error) => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("Invalid UTF-8 bulk body: {error}"),
                            ));
                        }
                    };
                    let mut tail = [0; 3];
                    tail[..incomplete].copy_from_slice(&self.utf8[self.utf8.len() - incomplete..]);
                    self.utf8.clear();
                    self.utf8.extend_from_slice(&tail[..incomplete]);
                    self.spool.append_frame(&bytes)?;
                    crate::metrics::BULK_STAGED_INPUT_BYTES_TOTAL.inc_by(bytes.len() as u64);
                    self.current = bytes;
                }
                Some(Input::End) => {
                    if !self.utf8.is_empty() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Invalid UTF-8 bulk body: incomplete final code point",
                        ));
                    }
                    self.ended = true;
                    return Ok(0);
                }
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::ConnectionAborted,
                        "Bulk request cancelled before the complete body was validated",
                    ));
                }
            }
        }
        let count = output.len().min(self.current.len());
        output[..count].copy_from_slice(&self.current[..count]);
        self.current = self.current.slice(count..);
        Ok(count)
    }
}

pub(super) struct StagedBulk {
    pub decoder: BulkDecoder<VerifiedInput>,
    pub item_count: u64,
    pub only_default_index: bool,
}

pub(super) async fn stage(
    body: Body,
    directory: PathBuf,
    default_index: Option<String>,
    runtime: &BulkRuntime,
    lease: Arc<RequestLease>,
) -> Result<StagedBulk, BulkError> {
    let (sender, receiver) = mpsc::channel(2);
    let maximum = runtime.max_request_bytes;
    let mut worker = tokio::task::spawn_blocking(move || {
        let spool = Spool::new(&directory, lease)?;
        let reader = InputReader {
            receiver,
            current: Bytes::new(),
            spool,
            input_bytes: 0,
            maximum,
            utf8: Vec::with_capacity(IO_BYTES + 3),
            ended: false,
        };
        let mut decoder = BulkDecoder::new(BufReader::with_capacity(IO_BYTES, reader));
        let mut indices = HashSet::new();
        let mut item_count = 0;
        let mut only_default_index = true;
        while let Some(document) = decoder.next_document()? {
            item_count += 1;
            only_default_index &=
                document.index.as_deref().or(default_index.as_deref()) == default_index.as_deref();
            if validate_bulk_document(&document).is_ok()
                && let Some(index) = document.index.as_deref().or(default_index.as_deref())
                && crate::common::IndexName::new(index.to_string()).is_ok()
            {
                indices.insert(index.to_string());
                if indices.len() > MAX_INDICES {
                    return Err(BulkError::Limit(format!(
                        "Bulk request exceeds {MAX_INDICES} distinct valid indices"
                    )));
                }
            }
        }
        let mut spool = decoder.reader.into_inner().spool;
        spool.rewind()?;
        Ok(StagedBulk {
            decoder: BulkDecoder::new(VerifiedInput {
                spool,
                bytes: Vec::new(),
                cursor: 0,
            }),
            item_count,
            only_default_index,
        })
    });
    let timeout = runtime.idle_timeout;
    let mut pump = Box::pin(async move {
        let mut stream = body.into_data_stream();
        while let Some(frame) = tokio::time::timeout(timeout, stream.next())
            .await
            .map_err(|_| BulkError::Timeout)?
        {
            let frame = frame.map_err(|error| {
                BulkError::Io(io::Error::other(format!("Reading bulk HTTP body: {error}")))
            })?;
            for bytes in frame.chunks(IO_BYTES) {
                if sender
                    .send(Input::Data(Bytes::copy_from_slice(bytes)))
                    .await
                    .is_err()
                {
                    return Ok(());
                }
            }
        }
        let _ = sender.send(Input::End).await;
        Ok::<_, BulkError>(())
    });
    tokio::select! {
        result = &mut worker => result?,
        result = &mut pump => {
            match result {
                Ok(()) => worker.await?,
                Err(error) => {
                    drop(pump);
                    match worker.await {
                        Ok(Err(BulkError::Io(cleanup)))
                            if cleanup.kind() == io::ErrorKind::ConnectionAborted => {}
                        Ok(Err(cleanup)) => {
                            tracing::error!(error = %cleanup, "Bulk staging failed during intake cleanup");
                        }
                        Err(cleanup) => {
                            tracing::error!(error = %cleanup, "Bulk staging worker failed during intake cleanup");
                        }
                        Ok(Ok(_)) => {}
                    }
                    Err(error)
                }
            }
        }
    }
}

pub(super) async fn next_batch(
    mut decoder: BulkDecoder<VerifiedInput>,
) -> Result<(BulkDecoder<VerifiedInput>, Vec<BulkDoc>), BulkError> {
    tokio::task::spawn_blocking(move || {
        let start = decoder.bytes_read;
        let mut documents = Vec::new();
        while documents.len() < BATCH_ITEMS && decoder.bytes_read - start < BATCH_BYTES {
            let Some(document) = decoder.next_document()? else {
                break;
            };
            documents.push(document);
        }
        Ok((decoder, documents))
    })
    .await?
}

#[derive(Serialize, Deserialize)]
pub(super) struct StoredResult {
    pub item: Value,
    pub target: Option<BulkTargetKey>,
}

pub(super) async fn store_results(
    mut spool: Spool,
    results: Vec<StoredResult>,
) -> Result<Spool, BulkError> {
    tokio::task::spawn_blocking(move || {
        for result in results {
            spool.append_result(&result)?;
        }
        Ok(spool)
    })
    .await?
}

pub(super) async fn result_spool(
    directory: PathBuf,
    lease: Arc<RequestLease>,
) -> Result<Spool, BulkError> {
    Ok(tokio::task::spawn_blocking(move || Spool::new(&directory, lease)).await??)
}

pub(super) async fn finish_results(mut spool: Spool) -> Result<Spool, BulkError> {
    tokio::task::spawn_blocking(move || {
        spool.rewind()?;
        Ok(spool)
    })
    .await?
}

enum ResponseFrame {
    Data(Bytes),
    Complete,
}

struct ResponseReceiver {
    receiver: mpsc::Receiver<ResponseFrame>,
    failure: Arc<Mutex<Option<String>>>,
    lease: Arc<RequestLease>,
    finished: bool,
}

impl Drop for ResponseReceiver {
    fn drop(&mut self) {
        if self.lease.terminal.load(Ordering::Acquire) == 0 {
            self.lease.cancelled.store(true, Ordering::Release);
        }
    }
}

pub(super) fn response(
    mut spool: Spool,
    reports: HashMap<BulkTargetKey, crate::transport::proto::ShardWriteRefreshResult>,
    errors: bool,
    pretty: bool,
    runtime: &BulkRuntime,
    lease: Arc<RequestLease>,
) -> Response {
    let (sender, receiver) = mpsc::channel(2);
    let failure = Arc::new(Mutex::new(None));
    let producer_failure = failure.clone();
    let timeout = runtime.idle_timeout;
    let handle = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        let result = (|| -> Result<(), BulkError> {
            let send = |frame| -> Result<(), BulkError> {
                handle
                    .block_on(sender.send_timeout(frame, timeout))
                    .map_err(|error| {
                        BulkError::Io(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            format!("Bulk response consumer closed or stalled: {error}"),
                        ))
                    })
            };
            let prefix = if pretty {
                format!("{{\n  \"took\": 0,\n  \"errors\": {errors},\n  \"items\": [")
            } else {
                format!("{{\"took\":0,\"errors\":{errors},\"items\":[")
            };
            send(ResponseFrame::Data(Bytes::from(prefix)))?;
            let mut first = true;
            while let Some(bytes) = spool.read_frame(MAX_RESULT_BYTES)? {
                let StoredResult { mut item, target } = serde_json::from_slice(&bytes)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                if let Some(key) = target
                    && let Some(report) = reports.get(&key)
                {
                    let result = item
                        .as_object_mut()
                        .and_then(|object| object.values_mut().next())
                        .ok_or_else(|| {
                            BulkError::Invalid("Bulk result spool has no action result".into())
                        })?;
                    if result.get("error").is_none() {
                        crate::transport::write_refresh::add_write_refresh_to_response(
                            result, &key.0, key.2, report,
                        );
                    }
                }
                let mut bytes = Vec::new();
                if !first {
                    bytes.push(b',');
                }
                if pretty {
                    bytes.push(b'\n');
                    serde_json::to_writer_pretty(&mut bytes, &item)
                } else {
                    serde_json::to_writer(&mut bytes, &item)
                }
                .map_err(io::Error::other)?;
                for chunk in bytes.chunks(IO_BYTES) {
                    send(ResponseFrame::Data(Bytes::copy_from_slice(chunk)))?;
                }
                first = false;
            }
            send(ResponseFrame::Data(Bytes::from_static(if pretty {
                b"\n  ]\n}\n"
            } else {
                b"]}"
            })))?;
            send(ResponseFrame::Complete)?;
            Ok(())
        })();
        if let Err(error) = result {
            if !matches!(
                &error,
                BulkError::Io(cause)
                    if matches!(cause.kind(), io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionAborted)
            ) {
                spool.lease.fail();
            }
            tracing::error!(error = %error, "Streaming bulk response aborted");
            *producer_failure.lock().expect("bulk response failure lock") = Some(error.to_string());
        }
    });
    let receiver = ResponseReceiver {
        receiver,
        failure,
        lease,
        finished: false,
    };
    let stream = futures::stream::unfold(receiver, |mut state| async move {
        if state.finished {
            return None;
        }
        match state.receiver.recv().await {
            Some(ResponseFrame::Data(bytes)) => Some((Ok::<_, io::Error>(bytes), state)),
            Some(ResponseFrame::Complete) => {
                state.lease.terminal.store(2, Ordering::Release);
                None
            }
            None => {
                state.finished = true;
                let reason = state
                    .failure
                    .lock()
                    .expect("bulk response failure lock")
                    .clone()
                    .unwrap_or_else(|| "Bulk response worker stopped before completion".into());
                Some((Err(io::Error::other(reason)), state))
            }
        }
    });
    let mut response = Body::from_stream(stream).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/json; charset=utf-8"),
    );
    response
        .extensions_mut()
        .insert(crate::api::BulkStreamingResponse);
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;
    use futures::stream;

    async fn released(runtime: &BulkRuntime) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while runtime.permits.available_permits() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("bulk request resources released");
    }

    fn runtime() -> Arc<BulkRuntime> {
        Arc::new(
            BulkRuntime::new(&AppConfig {
                bulk_max_concurrent_requests: 1,
                ..Default::default()
            })
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn bulk_stage_validates_every_byte_boundary_and_releases_anonymous_spools() {
        let directory = tempfile::tempdir().unwrap();
        let input = "{\"index\":{\"_id\":\"x\",\"_operation_id\":\"stable\"}}\n\
                     {\"body\":\"snow \u{96ea}\"}\n{\"delete\":{\"_id\":\"x\"}}\n";
        for size in 1..=input.len() {
            let runtime = runtime();
            let lease = runtime.acquire().unwrap();
            let chunks = input
                .as_bytes()
                .chunks(size)
                .map(|chunk| Ok::<_, io::Error>(Bytes::copy_from_slice(chunk)))
                .collect::<Vec<_>>();
            let staged = stage(
                Body::from_stream(stream::iter(chunks)),
                directory.path().to_path_buf(),
                Some("idx".into()),
                &runtime,
                lease.clone(),
            )
            .await
            .unwrap();
            assert_eq!(staged.item_count, 2);
            assert!(staged.only_default_index);
            let (decoder, documents) = next_batch(staged.decoder).await.unwrap();
            assert_eq!(documents.len(), 2);
            assert_eq!(documents[0].payload["body"], "snow \u{96ea}");
            assert_eq!(documents[0].operation_id.as_deref(), Some("stable"));
            assert_eq!(documents[1].action, "delete");
            lease.complete();
            drop(decoder);
            drop(lease);
            released(&runtime).await;
        }
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn bulk_stage_never_accepts_partial_body_utf8_or_input_spool_limits() {
        for (max_input, max_spool, input, expected) in [
            (
                8,
                1024,
                b"{\"delete\":{\"_id\":\"x\"}}\n".as_slice(),
                io::ErrorKind::FileTooLarge,
            ),
            (
                1024,
                8,
                b"{\"delete\":{\"_id\":\"x\"}}\n".as_slice(),
                io::ErrorKind::StorageFull,
            ),
            (
                1024,
                1024,
                b"{\"index\":{}}\n\"\xf0\x9f".as_slice(),
                io::ErrorKind::InvalidData,
            ),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let runtime = BulkRuntime::new(&AppConfig {
                bulk_max_concurrent_requests: 1,
                bulk_max_request_bytes: max_input,
                bulk_max_spool_bytes: max_spool,
                ..Default::default()
            })
            .unwrap();
            let lease = runtime.acquire().unwrap();
            let result = stage(
                Body::from(Bytes::copy_from_slice(input)),
                directory.path().to_path_buf(),
                None,
                &runtime,
                lease.clone(),
            )
            .await;
            match result {
                Err(BulkError::Io(error)) => assert_eq!(error.kind(), expected, "{error}"),
                _ => panic!("expected {expected:?}"),
            }
            lease.fail();
            drop(lease);
            released(&runtime).await;
        }
    }

    #[tokio::test]
    async fn bulk_stage_idle_deadline_closes_the_owned_worker() {
        let directory = tempfile::tempdir().unwrap();
        let runtime = BulkRuntime::new(&AppConfig {
            bulk_max_concurrent_requests: 1,
            bulk_idle_timeout_ms: 20,
            ..Default::default()
        })
        .unwrap();
        let lease = runtime.acquire().unwrap();
        let result = stage(
            Body::from_stream(stream::pending::<Result<Bytes, io::Error>>()),
            directory.path().to_path_buf(),
            None,
            &runtime,
            lease.clone(),
        )
        .await;
        assert!(matches!(result, Err(BulkError::Timeout)));
        lease.fail();
        drop(lease);
        released(&runtime).await;
    }

    #[tokio::test]
    async fn bulk_cancelled_input_releases_worker_file_and_admission() {
        let directory = tempfile::tempdir().unwrap();
        let runtime = runtime();
        let lease = runtime.acquire().unwrap();
        let reached = Arc::new(tokio::sync::Notify::new());
        let observed = reached.clone();
        let body = Body::from_stream(
            stream::once(async move {
                observed.notify_one();
                Ok::<_, io::Error>(Bytes::from_static(b"{\"index\":{}}\n"))
            })
            .chain(stream::pending()),
        );
        let limits = runtime.clone();
        let path = directory.path().to_path_buf();
        let job = tokio::spawn(async move {
            let _scope = RequestScope::new(lease.clone());
            stage(body, path, Some("idx".into()), &limits, lease).await
        });
        reached.notified().await;
        assert!(runtime.acquire().is_err());
        job.abort();
        assert!(matches!(job.await, Err(error) if error.is_cancelled()));
        released(&runtime).await;
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn bulk_offloaded_io_keeps_admission_after_its_waiter_is_cancelled() {
        let directory = tempfile::tempdir().unwrap();
        let runtime = runtime();
        let lease = runtime.acquire().unwrap();
        let path = directory.path().to_path_buf();
        let (hit, reached) = tokio::sync::oneshot::channel();
        let (release, gate) = std::sync::mpsc::sync_channel(1);
        let job = tokio::spawn(async move {
            tokio::task::spawn_blocking(move || {
                let spool = Spool::new(&path, lease).unwrap();
                hit.send(()).unwrap();
                gate.recv().unwrap();
                drop(spool);
            })
            .await
        });
        reached.await.unwrap();
        job.abort();
        assert!(job.await.unwrap_err().is_cancelled());
        assert!(runtime.acquire().is_err());
        release.send(()).unwrap();
        released(&runtime).await;
    }

    #[tokio::test]
    async fn bulk_input_and_results_share_the_same_spool_reservation() {
        let directory = tempfile::tempdir().unwrap();
        let runtime = BulkRuntime::new(&AppConfig {
            bulk_max_concurrent_requests: 1,
            bulk_max_spool_bytes: 100,
            ..Default::default()
        })
        .unwrap();
        let lease = runtime.acquire().unwrap();
        let path = directory.path().to_path_buf();
        let owned = lease.clone();
        tokio::task::spawn_blocking(move || {
            let mut input = Spool::new(&path, owned.clone()).unwrap();
            let mut output = Spool::new(&path, owned).unwrap();
            input.write_all(&[0; 60]).unwrap();
            assert_eq!(
                output.write_all(&[0; 60]).unwrap_err().kind(),
                io::ErrorKind::StorageFull
            );
        })
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while lease.reserved.load(Ordering::Acquire) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        lease.complete();
        drop(lease);
        released(&runtime).await;
    }

    #[tokio::test]
    async fn bulk_spooled_response_preserves_order_and_pretty() {
        let directory = tempfile::tempdir().unwrap();
        for pretty in [false, true] {
            let runtime = runtime();
            let lease = runtime.acquire().unwrap();
            let spool = result_spool(directory.path().to_path_buf(), lease.clone())
                .await
                .unwrap();
            let items = (0..600)
                .map(|id| StoredResult {
                    item: serde_json::json!({"index":{"_id":id.to_string(),"status":201}}),
                    target: None,
                })
                .collect();
            let spool = store_results(spool, items).await.unwrap();
            let spool = finish_results(spool).await.unwrap();
            let response = response(
                spool,
                HashMap::new(),
                false,
                pretty,
                &runtime,
                lease.clone(),
            );
            assert!(
                response
                    .extensions()
                    .get::<crate::api::BulkStreamingResponse>()
                    .is_some()
            );
            drop(lease);
            let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
                .await
                .unwrap();
            let value: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(value["errors"], false);
            assert_eq!(value["items"].as_array().unwrap().len(), 600);
            for (id, item) in value["items"].as_array().unwrap().iter().enumerate() {
                assert_eq!(item["index"]["_id"], id.to_string());
            }
            assert_eq!(bytes.contains(&b'\n'), pretty);
            released(&runtime).await;
        }
    }

    #[tokio::test]
    async fn bulk_response_drop_cancels_the_bounded_producer_and_closes_spool() {
        let directory = tempfile::tempdir().unwrap();
        let runtime = runtime();
        let lease = runtime.acquire().unwrap();
        let spool = result_spool(directory.path().to_path_buf(), lease.clone())
            .await
            .unwrap();
        let spool = store_results(
            spool,
            (0..100)
                .map(|_| StoredResult {
                    item: serde_json::json!({"index":{"_id":"x".repeat(IO_BYTES)}}),
                    target: None,
                })
                .collect(),
        )
        .await
        .unwrap();
        let spool = finish_results(spool).await.unwrap();
        let response = response(spool, HashMap::new(), false, false, &runtime, lease.clone());
        drop(lease);
        let mut body = response.into_body().into_data_stream();
        assert!(body.next().await.unwrap().is_ok());
        assert!(runtime.acquire().is_err());
        drop(body);
        released(&runtime).await;
    }

    #[tokio::test]
    async fn bulk_corrupt_result_spool_aborts_the_body_instead_of_returning_success_json() {
        let directory = tempfile::tempdir().unwrap();
        let runtime = runtime();
        let lease = runtime.acquire().unwrap();
        let path = directory.path().to_path_buf();
        let owned = lease.clone();
        let spool = tokio::task::spawn_blocking(move || {
            let mut spool = Spool::new(&path, owned).unwrap();
            spool.write_all(b"{\"item\":").unwrap();
            spool.rewind().unwrap();
            spool
        })
        .await
        .unwrap();
        let response = response(spool, HashMap::new(), false, false, &runtime, lease.clone());
        drop(lease);
        let error = axum::body::to_bytes(response.into_body(), IO_BYTES)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("bulk spool frame"), "{error}");
        released(&runtime).await;
    }

    #[tokio::test]
    async fn bulk_spool_frames_reject_valid_json_corruption_and_exact_boundary_truncation() {
        for mode in ["changed-json", "truncated", "partial", "reordered"] {
            let directory = tempfile::tempdir().unwrap();
            let runtime = runtime();
            let lease = runtime.acquire().unwrap();
            let path = directory.path().to_path_buf();
            let owned = lease.clone();
            tokio::task::spawn_blocking(move || {
                let mut spool = Spool::new(&path, owned).unwrap();
                let original = b"{\"value\":1}";
                spool.append_frame(original).unwrap();
                spool.rewind().unwrap();
                let SpoolIo::Reading(reader) = spool.io.as_mut().unwrap() else {
                    panic!("sealed spool");
                };
                match mode {
                    "changed-json" => {
                        let digit = original.iter().position(|byte| *byte == b'1').unwrap();
                        reader
                            .get_mut()
                            .seek(SeekFrom::Start(44 + digit as u64))
                            .unwrap();
                        reader.get_mut().write_all(b"2").unwrap();
                        assert!(serde_json::from_slice::<Value>(b"{\"value\":2}").is_ok());
                    }
                    "truncated" => reader.get_mut().set_len(0).unwrap(),
                    "partial" => reader.get_mut().set_len(46).unwrap(),
                    "reordered" => {
                        reader.get_mut().seek(SeekFrom::Start(4)).unwrap();
                        reader.get_mut().write_all(&1_u64.to_le_bytes()).unwrap();
                    }
                    _ => unreachable!(),
                }
                reader.seek(SeekFrom::Start(0)).unwrap();
                let error = spool.read_frame(IO_BYTES).unwrap_err();
                match mode {
                    "changed-json" => assert!(error.to_string().contains("checksum")),
                    "truncated" => assert!(error.to_string().contains("truncated")),
                    "partial" => assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof),
                    "reordered" => assert!(error.to_string().contains("order")),
                    _ => unreachable!(),
                }
            })
            .await
            .unwrap();
            lease.fail();
            drop(lease);
            released(&runtime).await;
        }
    }

    #[tokio::test]
    async fn bulk_spool_frames_cannot_be_substituted_from_another_valid_file() {
        let directory = tempfile::tempdir().unwrap();
        let runtime = runtime();
        let lease = runtime.acquire().unwrap();
        let owned = lease.clone();
        let path = directory.path().to_path_buf();
        tokio::task::spawn_blocking(move || {
            let mut first = Spool::new(&path, owned.clone()).unwrap();
            let mut second = Spool::new(&path, owned).unwrap();
            for spool in [&mut first, &mut second] {
                spool.append_frame(b"{\"value\":1}").unwrap();
                spool.rewind().unwrap();
            }
            let mut bytes = Vec::new();
            let SpoolIo::Reading(reader) = first.io.as_mut().unwrap() else {
                panic!("sealed first spool");
            };
            reader.get_mut().read_to_end(&mut bytes).unwrap();
            let SpoolIo::Reading(reader) = second.io.as_mut().unwrap() else {
                panic!("sealed second spool");
            };
            reader.get_mut().write_all(&bytes).unwrap();
            reader.seek(SeekFrom::Start(0)).unwrap();
            assert!(
                second
                    .read_frame(IO_BYTES)
                    .unwrap_err()
                    .to_string()
                    .contains("checksum")
            );
        })
        .await
        .unwrap();
        lease.fail();
        drop(lease);
        released(&runtime).await;
    }
}
