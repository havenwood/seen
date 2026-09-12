use magnus::scan_args::scan_args;
use magnus::typed_data::Obj;
use magnus::value::{LazyId, Opaque};
use magnus::{
    DataTypeFunctions, Error, ExceptionClass, IntoValue, RArray, RHash, RModule, RString, Ruby,
    Symbol, TryConvert, TypedData, Value, function, gc, kwargs, method, prelude::*,
};
use seen_core::{
    FILE_TYPES, GrepConfig, GrepFormat, GrepMatch, GrepPosition, SearchConfig, SearchError,
    grep_stream, search_stream,
};
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::net::UnixStream;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex, Weak};

// rb-sys's tracking allocator calls Ruby's GC API. Search workers allocate on
// native threads without the GVL, so the process allocator must remain in use.

static PATTERN: LazyId = LazyId::new("pattern");
static PATHS: LazyId = LazyId::new("paths");
static HIDDEN: LazyId = LazyId::new("hidden");
static NO_IGNORE: LazyId = LazyId::new("no_ignore");
static CASE_SENSITIVE: LazyId = LazyId::new("case_sensitive");
static CONTENT_CASE_SENSITIVE: LazyId = LazyId::new("content_case_sensitive");
static GLOB: LazyId = LazyId::new("glob");
static FULL_PATH: LazyId = LazyId::new("full_path");
static FOLLOW: LazyId = LazyId::new("follow");
static MAX_DEPTH: LazyId = LazyId::new("max_depth");
static MIN_DEPTH: LazyId = LazyId::new("min_depth");
static TYPE: LazyId = LazyId::new("type");
static EXTENSION: LazyId = LazyId::new("extension");
static EXCLUDE: LazyId = LazyId::new("exclude");
static MIN_SIZE: LazyId = LazyId::new("min_size");
static MAX_SIZE: LazyId = LazyId::new("max_size");
static CHANGED_WITHIN: LazyId = LazyId::new("changed_within");
static CHANGED_BEFORE: LazyId = LazyId::new("changed_before");
static NAME: LazyId = LazyId::new("name");
static STRIP_CWD_PREFIX: LazyId = LazyId::new("strip_cwd_prefix");
static IGNORE_ERROR: LazyId = LazyId::new("ignore_error");
static IGNORE_FILE: LazyId = LazyId::new("ignore_file");
static TEXT: LazyId = LazyId::new("text");
static COLUMN: LazyId = LazyId::new("column");
static BYTE_RANGE: LazyId = LazyId::new("byte_range");
static MAX_COUNT: LazyId = LazyId::new("max_count");
static HEAP_LIMIT: LazyId = LazyId::new("heap_limit");
static ENCODING: LazyId = LazyId::new("encoding");

/// Rejects before coercion so caller `to_str` and `to_ary` errors remain intact.
fn reject_type(ruby: &Ruby, value: Value, target: &str) -> Error {
    seen_error(
        ruby,
        "InvalidType",
        ruby.exception_type_error(),
        format!("no implicit conversion of {} into {target}", value.class()),
    )
}

fn coercible(value: Value, coercions: &[&str]) -> Result<bool, Error> {
    for name in coercions {
        if value.respond_to(*name, true)? {
            return Ok(true);
        }
    }

    Ok(false)
}

/// Accepts valid UTF-8 binary strings that magnus cannot transcode while
/// preserving coercion errors.
fn to_utf8(ruby: &Ruby, value: Value, name: &str) -> Result<String, Error> {
    let Some(string) = RString::from_value(value) else {
        return String::try_convert(value);
    };
    let invalid = || {
        seen_error(
            ruby,
            "InvalidOption",
            ruby.exception_arg_error(),
            format!("{name} must be valid UTF-8"),
        )
    };

    if string.enc_get() == ruby.ascii8bit_encindex() {
        return String::from_utf8(string.to_bytes().into()).map_err(|_| invalid());
    }

    string.to_string().map_err(|_| invalid())
}

fn extract_string(ruby: &Ruby, hash: RHash, key: &LazyId) -> Result<Option<String>, Error> {
    let Some(value) = hash.get(**key).filter(|value| !value.is_nil()) else {
        return Ok(None);
    };

    if !value.is_kind_of(ruby.class_string()) && !coercible(value, &["to_str"])? {
        return Err(reject_type(ruby, value, "String"));
    }

    to_utf8(ruby, value, LazyId::get_inner_with(key, ruby).name()?).map(Some)
}

fn extract_optional_arg<T: TryConvert>(hash: RHash, key: &LazyId) -> Result<Option<T>, Error> {
    hash.get(**key)
        .filter(|val| !val.is_nil())
        .map(TryConvert::try_convert)
        .transpose()
}

fn extract_boolish(hash: RHash, key: &LazyId, default: bool) -> Result<bool, Error> {
    hash.get(**key)
        .map(bool::try_convert)
        .transpose()
        .map(|value| value.unwrap_or(default))
}

enum Outcome {
    Done(Result<(), SearchError>),
    Panicked(Box<dyn std::any::Any + Send>),
}

enum StreamEvent {
    Search(Vec<Vec<u8>>),
    Grep(Vec<GrepMatch>),
}

enum ActiveEvent {
    Search(std::vec::IntoIter<Vec<u8>>),
    Grep(std::vec::IntoIter<GrepMatch>),
}

impl From<StreamEvent> for ActiveEvent {
    fn from(event: StreamEvent) -> Self {
        match event {
            StreamEvent::Search(paths) => Self::Search(paths.into_iter()),
            StreamEvent::Grep(matches) => Self::Grep(matches.into_iter()),
        }
    }
}

enum StreamItem {
    Search(Vec<u8>),
    Grep(GrepMatch),
}

/// Only Ruby values and integers may remain on the native stack while yielding.
/// In particular, Ruby can discard an Enumerator's Fiber without running Rust
/// destructors, so all owned buffers must stay in its GC-managed cursor.
enum PreparedItem {
    Path(RString),
    Line(RString, u64, RString),
    Column(RString, u64, u64, RString),
    ByteRange(RString, u64, magnus::Range, RString),
}

impl PreparedItem {
    fn yield_values(self, ruby: &Ruby) -> Result<Value, Error> {
        match self {
            Self::Path(path) => ruby.yield_value(path),
            Self::Line(path, number, text) => ruby.yield_values((path, number, text)),
            Self::Column(path, number, column, text) => {
                ruby.yield_values((path, number, column, text))
            }
            Self::ByteRange(path, number, range, text) => {
                ruby.yield_values((path, number, range, text))
            }
        }
    }

    fn into_values(self, ruby: &Ruby) -> Value {
        match self {
            Self::Path(path) => (path,).into_value_with(ruby),
            Self::Line(path, number, text) => (path, number, text).into_value_with(ruby),
            Self::Column(path, number, column, text) => {
                (path, number, column, text).into_value_with(ruby)
            }
            Self::ByteRange(path, number, range, text) => {
                (path, number, range, text).into_value_with(ruby)
            }
        }
    }
}

impl ActiveEvent {
    fn next(&mut self) -> Option<StreamItem> {
        match self {
            Self::Search(paths) => paths.next().map(StreamItem::Search),
            Self::Grep(matches) => matches.next().map(StreamItem::Grep),
        }
    }
}

struct StreamState {
    events: VecDeque<StreamEvent>,
    outcome: Option<Outcome>,
}

enum StreamNext {
    Event(StreamEvent),
    Outcome(Outcome),
}

struct StreamSignal {
    reader: UnixStream,
    writer: UnixStream,
}

impl StreamSignal {
    fn new() -> std::io::Result<Self> {
        let (reader, writer) = UnixStream::pair()?;
        reader.set_nonblocking(true)?;
        writer.set_nonblocking(true)?;
        Ok(Self { reader, writer })
    }

    fn reader_fd(&self) -> RawFd {
        self.reader.as_raw_fd()
    }

    fn notify(&self) {
        drop((&self.writer).write(&[1]));
    }

    fn drain(&self) {
        let mut bytes = [0; 64];
        loop {
            match (&self.reader).read(&mut bytes) {
                Ok(0) => break,
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(_) => break,
            }
        }
    }
}

impl StreamState {
    fn take_next(&mut self) -> Option<StreamNext> {
        if let Some(event) = self.events.pop_front() {
            return Some(StreamNext::Event(event));
        }

        self.outcome.take().map(StreamNext::Outcome)
    }
}

struct StreamSession {
    pid: u32,
    state: Mutex<StreamState>,
    space: Condvar,
    cancelled: AtomicBool,
    capacity: usize,
    signal: StreamSignal,
}

impl StreamSession {
    fn new() -> std::io::Result<Self> {
        let capacity = seen_core::queue_capacity();

        Ok(Self {
            pid: std::process::id(),
            state: Mutex::new(StreamState {
                events: VecDeque::with_capacity(capacity),
                outcome: None,
            }),
            space: Condvar::new(),
            cancelled: AtomicBool::new(false),
            capacity,
            signal: StreamSignal::new()?,
        })
    }

    fn inherited(&self) -> bool {
        self.pid != std::process::id()
    }

    fn cancelled() -> StreamNext {
        StreamNext::Outcome(Outcome::Done(Err(SearchError::Cancelled)))
    }

    fn push(&self, event: StreamEvent) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while state.events.len() >= self.capacity && !self.cancelled.load(Ordering::Relaxed) {
            state = self
                .space
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        if self.cancelled.load(Ordering::Relaxed) {
            return false;
        }

        state.events.push_back(event);
        drop(state);
        self.signal();
        true
    }

    fn finish(&self, outcome: Outcome) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.cancelled.load(Ordering::Relaxed) {
            return;
        }
        state.outcome = Some(outcome);
        drop(state);
        self.signal();
    }

    /// Locks before notifying so cancellation cannot race a producer's wait.
    fn cancel(&self) {
        if self.inherited() {
            return;
        }

        self.cancelled.store(true, Ordering::Relaxed);
        let payload = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (std::mem::take(&mut state.events), state.outcome.take())
        };
        drop(payload);
        self.space.notify_all();
        self.signal();
    }

    fn signal(&self) {
        self.signal.notify();
    }

    fn signal_fd(&self) -> RawFd {
        self.signal.reader_fd()
    }

    fn drain_signal(&self) {
        self.signal.drain();
    }

    fn take_ready(&self) -> Option<StreamNext> {
        if self.inherited() || self.cancelled.load(Ordering::Relaxed) {
            return Some(Self::cancelled());
        }

        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let next = state.take_next()?;
        let released = matches!(&next, StreamNext::Event(_));
        drop(state);
        if released {
            self.space.notify_one();
        }
        Some(next)
    }

    /// Uses Magnus's safe MRI wrapper to wait without blocking other threads.
    fn wait(&self, ruby: &Ruby) -> Result<StreamNext, Error> {
        loop {
            if let Some(next) = self.take_ready() {
                return Ok(next);
            }

            self.drain_signal();
            if let Some(next) = self.take_ready() {
                return Ok(next);
            }
            ruby.thread_wait_fd(&self.signal.reader)?;
        }
    }
}

enum StreamConfig {
    Search(SearchConfig),
    Grep(GrepConfig),
}

#[derive(TypedData)]
#[magnus(class = "Seen::Stream", free_immediately)]
struct StreamSource {
    config: Arc<StreamConfig>,
    sessions: Mutex<Vec<Weak<StreamSession>>>,
}

impl DataTypeFunctions for StreamSource {}

impl StreamSource {
    fn new(config: StreamConfig) -> Self {
        Self {
            config: Arc::new(config),
            sessions: Mutex::new(Vec::new()),
        }
    }

    fn operation(&self) -> &'static str {
        match self.config.as_ref() {
            StreamConfig::Search(_) => "Path search",
            StreamConfig::Grep(_) => "Line search",
        }
    }

    fn thread_name(&self) -> &'static str {
        match self.config.as_ref() {
            StreamConfig::Search(_) => "seen-path",
            StreamConfig::Grep(_) => "seen-line",
        }
    }

    fn start(&self, ruby: &Ruby) -> Result<Arc<StreamSession>, Error> {
        let session = Arc::new(StreamSession::new().map_err(|error| {
            seen_error(
                ruby,
                "IOError",
                ruby.exception_io_error(),
                format!("could not create the search signal: {error}"),
            )
        })?);
        let worker_session = Arc::clone(&session);
        let config = Arc::clone(&self.config);

        std::thread::Builder::new()
            .name(self.thread_name().to_owned())
            .spawn(move || {
                let outcome = catch_unwind(AssertUnwindSafe(|| match config.as_ref() {
                    StreamConfig::Search(config) => {
                        search_stream(config, &worker_session.cancelled, |paths| {
                            worker_session.push(StreamEvent::Search(paths))
                        })
                    }
                    StreamConfig::Grep(config) => {
                        grep_stream(config, &worker_session.cancelled, |matches| {
                            worker_session.push(StreamEvent::Grep(matches))
                        })
                    }
                }));
                worker_session.finish(match outcome {
                    Ok(result) => Outcome::Done(result),
                    Err(panic) => Outcome::Panicked(panic),
                });
            })
            .map_err(|error| {
                seen_error(
                    ruby,
                    "IOError",
                    ruby.exception_io_error(),
                    format!("could not start the search thread: {error}"),
                )
            })?;

        let mut sessions = self
            .sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        sessions.retain(|session| session.strong_count() > 0);
        sessions.push(Arc::downgrade(&session));
        drop(sessions);
        Ok(session)
    }
}

impl Drop for StreamSource {
    fn drop(&mut self) {
        let sessions = self
            .sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for session in sessions.iter().filter_map(Weak::upgrade) {
            session.cancel();
        }
    }
}

struct CloseCursor<'a>(&'a StreamCursor);

impl Drop for CloseCursor<'_> {
    fn drop(&mut self) {
        self.0.close();
    }
}

/// Owns every iteration's buffers and session, including native block iteration
/// reached through composed Enumerators. GC closes a discarded Fiber's cursor
/// even when Ruby cannot unwind the native yield loop.
#[derive(TypedData)]
#[magnus(class = "Seen::Cursor", free_immediately, mark)]
struct StreamCursor {
    operation: &'static str,
    session: RefCell<Option<Arc<StreamSession>>>,
    active: RefCell<Option<ActiveEvent>>,
    line: RefCell<Option<Arc<[u8]>>>,
    text: Cell<Option<Opaque<RString>>>,
    scheduler: Option<(Opaque<Value>, Opaque<Value>)>,
    scheduler_items: Cell<usize>,
}

impl DataTypeFunctions for StreamCursor {
    fn mark(&self, marker: &gc::Marker) {
        if let Some(text) = self.text.get() {
            marker.mark(text);
        }
        if let Some((scheduler, io)) = self.scheduler {
            marker.mark(scheduler);
            marker.mark(io);
        }
    }
}

impl StreamCursor {
    fn close(&self) {
        if let Some(session) = self.session.borrow_mut().take() {
            session.cancel();
        }
        drop(self.active.borrow_mut().take());
        drop(self.line.borrow_mut().take());
        self.text.set(None);
    }

    fn next_item(&self, ruby: &Ruby) -> Result<Option<PreparedItem>, Error> {
        let Some(session) = self.session.borrow().as_ref().map(Arc::clone) else {
            return Ok(None);
        };
        let wait = self.scheduler.map(|(scheduler, io)| SchedulerWait {
            scheduler: ruby.get_inner(scheduler),
            io: ruby.get_inner(io),
        });
        let item = loop {
            let item = self
                .active
                .borrow_mut()
                .as_mut()
                .and_then(ActiveEvent::next);
            if let Some(item) = item {
                break item;
            }
            drop(self.active.borrow_mut().take());
            let next = wait_next(ruby, &session, wait.as_ref())?;
            match next {
                StreamNext::Event(event) => *self.active.borrow_mut() = Some(event.into()),
                StreamNext::Outcome(Outcome::Done(result)) => {
                    self.close();
                    result.map_err(|error| core_error(ruby, self.operation, &error))?;
                    return Ok(None);
                }
                StreamNext::Outcome(Outcome::Panicked(panic)) => resume_unwind(panic),
            }
        };
        ruby.thread_check_ints()?;
        let mut line = self
            .line
            .borrow()
            .as_ref()
            .map(Arc::clone)
            .zip(self.text.get())
            .map(|(bytes, text)| (bytes, ruby.get_inner(text)));
        let prepared = prepare_stream_item(ruby, item, &mut line)?;
        if let Some((bytes, text)) = line {
            *self.line.borrow_mut() = Some(bytes);
            self.text.set(Some(text.into()));
        }
        if let Some(wait) = wait {
            let items = self.scheduler_items.get() + 1;
            self.scheduler_items.set(items % SCHEDULER_YIELD_ITEMS);
            if items == SCHEDULER_YIELD_ITEMS {
                wait.yield_now()?;
            }
        }
        Ok(Some(prepared))
    }

    fn next_values(ruby: &Ruby, rb_self: Obj<Self>) -> Result<Value, Error> {
        Ok(rb_self
            .next_item(ruby)?
            .map_or_else(|| ruby.qnil().as_value(), |item| item.into_values(ruby)))
    }
}

impl Drop for StreamCursor {
    fn drop(&mut self) {
        self.close();
    }
}

fn stream_cursor(ruby: &Ruby, source: &StreamSource) -> Result<Obj<StreamCursor>, Error> {
    let session = source.start(ruby)?;
    let mut cursor = StreamCursor {
        operation: source.operation(),
        session: RefCell::new(Some(Arc::clone(&session))),
        active: RefCell::new(None),
        line: RefCell::new(None),
        text: Cell::new(None),
        scheduler: None,
        scheduler_items: Cell::new(0),
    };
    cursor.scheduler = current_scheduler(ruby)?
        .map(|scheduler| SchedulerWait::new(ruby, &session, scheduler))
        .transpose()?
        .map(|wait| (wait.scheduler.into(), wait.io.into()));
    Ok(ruby.obj_wrap(cursor))
}

fn convert_array<T: TryConvert>(array: RArray) -> Result<Vec<T>, Error> {
    let len = array.len();
    let mut values = Vec::with_capacity(len);

    for index in 0..len {
        values.push(array.entry(index.cast_signed())?);
    }

    Ok(values)
}

/// Rejects non-strings like `paths` so they raise `Seen::Error`.
fn extract_strings(ruby: &Ruby, hash: RHash, key: &LazyId) -> Result<Vec<String>, Error> {
    let Some(value) = hash.get(**key).filter(|value| !value.is_nil()) else {
        return Ok(Vec::new());
    };

    if !value.is_kind_of(ruby.class_array()) && !coercible(value, &["to_ary"])? {
        return Err(reject_type(ruby, value, "Array"));
    }

    strings_from_array(ruby, RArray::try_convert(value)?, key)
}

fn strings_from_array(ruby: &Ruby, array: RArray, key: &LazyId) -> Result<Vec<String>, Error> {
    let len = array.len();

    for index in 0..len {
        let element: Value = array.entry(index.cast_signed())?;
        if !element.is_kind_of(ruby.class_string()) && !coercible(element, &["to_str"])? {
            return Err(reject_type(ruby, element, "String"));
        }
    }

    let name = LazyId::get_inner_with(key, ruby).name()?;
    let mut values = Vec::with_capacity(len);

    for index in 0..len {
        let element: Value = array.entry(index.cast_signed())?;
        values.push(to_utf8(ruby, element, name)?);
    }

    Ok(values)
}

fn extract_string_or_strings(ruby: &Ruby, hash: RHash, key: &LazyId) -> Result<Vec<String>, Error> {
    let Some(value) = hash.get(**key).filter(|value| !value.is_nil()) else {
        return Ok(Vec::new());
    };
    let name = LazyId::get_inner_with(key, ruby).name()?;

    if value.is_kind_of(ruby.class_string()) || coercible(value, &["to_str"])? {
        return to_utf8(ruby, value, name).map(|value| vec![value]);
    }
    if !value.is_kind_of(ruby.class_array()) && !coercible(value, &["to_ary"])? {
        return Err(reject_type(ruby, value, "String or Array"));
    }

    strings_from_array(ruby, RArray::try_convert(value)?, key)
}

fn extract_paths(ruby: &Ruby, hash: RHash, key: &LazyId) -> Result<Vec<std::path::PathBuf>, Error> {
    let Some(value) = hash.get(**key) else {
        return Ok(Vec::new());
    };

    if !value.is_kind_of(ruby.class_array()) && !coercible(value, &["to_ary"])? {
        return Err(reject_type(ruby, value, "Array"));
    }

    let array = RArray::try_convert(value)?;
    let len = array.len();

    for index in 0..len {
        let element: Value = array.entry(index.cast_signed())?;
        if !element.is_kind_of(ruby.class_string()) && !coercible(element, &["to_str", "to_path"])?
        {
            return Err(reject_type(ruby, element, "String"));
        }
    }

    convert_array(array)
}

/// Extracts an optional non-negative `Integer`, rejecting `Float` and other
/// numerics that `i64` conversion would silently truncate.
fn non_negative<T: TryFrom<i64>>(
    ruby: &Ruby,
    kwargs: RHash,
    key: &LazyId,
    name: &str,
) -> Result<Option<T>, Error> {
    let Some(value) = kwargs.get(**key).filter(|value| !value.is_nil()) else {
        return Ok(None);
    };

    if !value.is_kind_of(ruby.class_integer()) {
        return Err(seen_error(
            ruby,
            "InvalidType",
            ruby.exception_type_error(),
            format!("no implicit conversion of {} into Integer", value.class()),
        ));
    }

    let number = i64::try_convert(value).map_err(|_| {
        seen_error(
            ruby,
            "OutOfRange",
            ruby.exception_range_error(),
            "bignum too big to convert into 'long long'".to_owned(),
        )
    })?;

    T::try_from(number)
        .ok()
        .filter(|_| number >= 0)
        .map(Some)
        .ok_or_else(|| {
            seen_error(
                ruby,
                "InvalidOption",
                ruby.exception_arg_error(),
                format!("{name} must be a non-negative integer, got {number}"),
            )
        })
}

fn file_type_name(ruby: &Ruby, value: Value) -> Result<String, Error> {
    let file_type = if let Some(symbol) = Symbol::from_value(value) {
        symbol.name()?.into_owned()
    } else {
        to_utf8(ruby, value, "type")?
    };

    if FILE_TYPES.contains(&file_type.as_str()) {
        Ok(file_type)
    } else {
        Err(seen_error(
            ruby,
            "InvalidOption",
            ruby.exception_arg_error(),
            format!(
                "type must be one of {}, got {file_type}",
                FILE_TYPES.join(", ")
            ),
        ))
    }
}

fn extract_file_types(ruby: &Ruby, kwargs: RHash) -> Result<Vec<String>, Error> {
    let Some(value) = kwargs.get(*TYPE).filter(|value| !value.is_nil()) else {
        return Ok(Vec::new());
    };

    if Symbol::from_value(value).is_some()
        || value.is_kind_of(ruby.class_string())
        || coercible(value, &["to_str"])?
    {
        return file_type_name(ruby, value).map(|file_type| vec![file_type]);
    }
    if !value.is_kind_of(ruby.class_array()) && !coercible(value, &["to_ary"])? {
        return Err(reject_type(ruby, value, "String, Symbol, or Array"));
    }

    let array = RArray::try_convert(value)?;
    let len = array.len();
    let mut file_types = Vec::with_capacity(len);
    for index in 0..len {
        let element: Value = array.entry(index.cast_signed())?;
        if Symbol::from_value(element).is_none()
            && !element.is_kind_of(ruby.class_string())
            && !coercible(element, &["to_str"])?
        {
            return Err(reject_type(ruby, element, "String or Symbol"));
        }
        file_types.push(file_type_name(ruby, element)?);
    }

    Ok(file_types)
}

fn build_search_config(
    ruby: &Ruby,
    kwargs: RHash,
    pattern_key: &LazyId,
    file_type: Vec<String>,
) -> Result<SearchConfig, Error> {
    Ok(SearchConfig {
        pattern: extract_string(ruby, kwargs, pattern_key)?,
        paths: extract_paths(ruby, kwargs, &PATHS)?,
        strip_cwd_prefix: extract_optional_arg(kwargs, &STRIP_CWD_PREFIX)?.unwrap_or_default(),
        raise_on_error: !extract_boolish(kwargs, &IGNORE_ERROR, true)?,
        ignore_file: extract_paths(ruby, kwargs, &IGNORE_FILE)?,
        hidden: extract_optional_arg(kwargs, &HIDDEN)?.unwrap_or_default(),
        no_ignore: extract_optional_arg(kwargs, &NO_IGNORE)?.unwrap_or_default(),
        case_sensitive: extract_optional_arg(kwargs, &CASE_SENSITIVE)?.unwrap_or_default(),
        glob: extract_optional_arg(kwargs, &GLOB)?.unwrap_or_default(),
        full_path: extract_optional_arg(kwargs, &FULL_PATH)?.unwrap_or_default(),
        follow: extract_optional_arg(kwargs, &FOLLOW)?.unwrap_or_default(),
        max_depth: non_negative(ruby, kwargs, &MAX_DEPTH, "max_depth")?,
        min_depth: non_negative(ruby, kwargs, &MIN_DEPTH, "min_depth")?,
        file_type,
        extension: extract_string_or_strings(ruby, kwargs, &EXTENSION)?,
        exclude: extract_strings(ruby, kwargs, &EXCLUDE)?,
        min_size: non_negative(ruby, kwargs, &MIN_SIZE, "min_size")?,
        max_size: non_negative(ruby, kwargs, &MAX_SIZE, "max_size")?,
        changed_within: non_negative(ruby, kwargs, &CHANGED_WITHIN, "changed_within")?,
        changed_before: non_negative(ruby, kwargs, &CHANGED_BEFORE, "changed_before")?,
    })
}

/// Uses the stdlib parent when the `Seen` constant is missing.
fn seen_class(ruby: &Ruby, name: &str, fallback: ExceptionClass) -> ExceptionClass {
    ruby.class_object()
        .const_get::<_, RModule>("Seen")
        .and_then(|seen| seen.const_get::<_, ExceptionClass>(name))
        .unwrap_or(fallback)
}

fn seen_error(ruby: &Ruby, name: &str, fallback: ExceptionClass, message: String) -> Error {
    Error::new(seen_class(ruby, name, fallback), message)
}

/// Raw path in the filesystem encoding, or binary under an ASCII locale.
fn path_string(ruby: &Ruby, path: &[u8]) -> RString {
    if !path.is_ascii() && ruby.filesystem_encindex() == ruby.usascii_encindex() {
        return ruby.enc_str_new(path, ruby.ascii8bit_encoding());
    }

    ruby.enc_str_new(path, ruby.filesystem_encoding())
}

/// Decoded lines are UTF-8. Raw lines use the external encoding.
/// Every result is frozen so occurrences can safely share the same `String`.
fn line_string(ruby: &Ruby, line: &[u8], utf8: bool) -> RString {
    let encoding = if utf8 {
        ruby.utf8_encoding()
    } else {
        ruby.default_external_encoding()
    };
    let line = ruby.enc_str_new(line, encoding);
    line.freeze();
    line
}

fn core_error(ruby: &Ruby, operation: &str, error: &SearchError) -> Error {
    match error {
        SearchError::Cancelled => Error::new(
            ruby.exception_runtime_error(),
            format!("{operation} interrupted"),
        ),
        SearchError::InvalidRegex(_) => seen_error(
            ruby,
            "InvalidPattern",
            ruby.exception_regexp_error(),
            format!("{operation} failed: {error}"),
        ),
        SearchError::Io(_) => seen_error(
            ruby,
            "IOError",
            ruby.exception_io_error(),
            format!("{operation} failed: {error}"),
        ),
        SearchError::InvalidInput(_) => seen_error(
            ruby,
            "InvalidOption",
            ruby.exception_arg_error(),
            format!("{operation} failed: {error}"),
        ),
    }
}

struct SchedulerWait {
    scheduler: Value,
    io: Value,
}

const SCHEDULER_YIELD_ITEMS: usize = 1024;

impl SchedulerWait {
    fn new(ruby: &Ruby, session: &StreamSession, scheduler: Value) -> Result<Self, Error> {
        let fd = session.signal_fd();
        let io: Value = ruby
            .class_io()
            .funcall("for_fd", (fd, kwargs!(ruby, "autoclose" => false)))?;
        Ok(Self { scheduler, io })
    }

    fn wait(&self) -> Result<(), Error> {
        let _: Value = self.scheduler.funcall("io_wait", (self.io, 1))?;
        Ok(())
    }

    fn yield_now(&self) -> Result<(), Error> {
        let _: Value = self.scheduler.funcall("kernel_sleep", (0,))?;
        Ok(())
    }
}

fn prepare_stream_item(
    ruby: &Ruby,
    item: StreamItem,
    line: &mut Option<(Arc<[u8]>, RString)>,
) -> Result<PreparedItem, Error> {
    Ok(match item {
        StreamItem::Search(bytes) => {
            let path = path_string(ruby, &bytes);
            drop(bytes);
            PreparedItem::Path(path)
        }
        StreamItem::Grep(matched) => {
            let GrepMatch {
                path: path_bytes,
                line_number,
                position,
                text: line_bytes,
                utf8,
            } = matched;
            let path = path_string(ruby, &path_bytes);
            let text = if position.is_none() {
                line_string(ruby, line_bytes.as_ref(), utf8)
            } else {
                match line.as_ref() {
                    Some((cached, string)) if Arc::ptr_eq(cached, &line_bytes) => *string,
                    _ => {
                        let string = line_string(ruby, line_bytes.as_ref(), utf8);
                        *line = Some((Arc::clone(&line_bytes), string));
                        string
                    }
                }
            };
            drop(path_bytes);
            drop(line_bytes);
            match position {
                Some(GrepPosition::Column(column)) => {
                    PreparedItem::Column(path, line_number, column, text)
                }
                // A `Range` so `text.byteslice(range)` returns the match,
                // with the exclusive end `rg --json` reports.
                Some(GrepPosition::ByteRange { offset, length }) => {
                    let range = ruby.range_new(offset, offset.saturating_add(length), true)?;
                    PreparedItem::ByteRange(path, line_number, range, text)
                }
                None => PreparedItem::Line(path, line_number, text),
            }
        }
    })
}

fn current_scheduler(ruby: &Ruby) -> Result<Option<Value>, Error> {
    let fiber: Value = ruby.class_object().const_get("Fiber")?;
    let scheduler: Value = fiber.funcall("scheduler", ())?;
    let current: Value = fiber.funcall("current", ())?;
    Ok(
        (!scheduler.is_nil() && !current.funcall::<_, _, bool>("blocking?", ())?)
            .then_some(scheduler),
    )
}

fn wait_next(
    ruby: &Ruby,
    session: &StreamSession,
    wait: Option<&SchedulerWait>,
) -> Result<StreamNext, Error> {
    let Some(wait) = wait else {
        return session.wait(ruby);
    };
    loop {
        if let Some(next) = session.take_ready() {
            return Ok(next);
        }
        session.drain_signal();
        if let Some(next) = session.take_ready() {
            return Ok(next);
        }
        wait.wait()?;
    }
}

fn stream_each(ruby: &Ruby, rb_self: Value) -> Result<Value, Error> {
    let source: &StreamSource = TryConvert::try_convert(rb_self)?;
    let cursor = stream_cursor(ruby, source)?;
    let _close = CloseCursor(&cursor);
    while let Some(item) = cursor.next_item(ruby)? {
        item.yield_values(ruby)?;
    }
    Ok(ruby.qnil().as_value())
}

fn seen_each_path(ruby: &Ruby, args: &[Value]) -> Result<Obj<StreamSource>, Error> {
    let args_scan = scan_args::<(), (), (), (), RHash, ()>(args)?;
    let kwargs = args_scan.keywords;
    let file_type = extract_file_types(ruby, kwargs)?;
    let config = build_search_config(ruby, kwargs, &PATTERN, file_type)?;

    Ok(ruby.obj_wrap(StreamSource::new(StreamConfig::Search(config))))
}

fn seen_each_line(ruby: &Ruby, args: &[Value]) -> Result<Obj<StreamSource>, Error> {
    let args_scan = scan_args::<(), (), (), (), RHash, ()>(args)?;
    let kwargs = args_scan.keywords;
    if let Some(value) = kwargs.get(*PATTERN)
        && value.is_nil()
    {
        return Err(reject_type(ruby, value, "String"));
    }
    let pattern: String = extract_string(ruby, kwargs, &PATTERN)?.ok_or_else(|| {
        seen_error(
            ruby,
            "InvalidOption",
            ruby.exception_arg_error(),
            "missing keyword: pattern".to_owned(),
        )
    })?;
    let search = build_search_config(ruby, kwargs, &NAME, Vec::new())?;
    let content_case_sensitive = extract_boolish(kwargs, &CONTENT_CASE_SENSITIVE, true)?;

    let format = GrepFormat::from_options(
        extract_optional_arg(kwargs, &COLUMN)?.unwrap_or_default(),
        extract_optional_arg(kwargs, &BYTE_RANGE)?.unwrap_or_default(),
    )
    .map_err(|error| core_error(ruby, "Line search", &error))?;
    let config = GrepConfig {
        pattern,
        content_case_sensitive,
        text: extract_optional_arg(kwargs, &TEXT)?.unwrap_or_default(),
        max_count: non_negative(ruby, kwargs, &MAX_COUNT, "max_count")?,
        heap_limit: non_negative(ruby, kwargs, &HEAP_LIMIT, "heap_limit")?,
        encoding: extract_string(ruby, kwargs, &ENCODING)?,
        format,
        search,
    };
    Ok(ruby.obj_wrap(StreamSource::new(StreamConfig::Grep(config))))
}

#[magnus::init]
#[allow(
    unsafe_code,
    reason = "MRI exposes Ractor-safety registration as an unsafe C API"
)]
fn init(ruby: &Ruby) -> Result<(), Error> {
    // SAFETY: workers share no Ruby values across threads or Ractors.
    unsafe { rb_sys::rb_ext_ractor_safe(true) };

    let seen_module = ruby.define_module("Seen")?;
    let error = seen_module.define_module("Error")?;
    let stream = seen_module.define_class("Stream", ruby.class_object())?;
    stream.define_method("each", method!(stream_each, 0))?;
    stream.define_method("cursor", method!(stream_cursor, 0))?;
    let cursor = seen_module.define_class("Cursor", ruby.class_object())?;
    cursor.define_method("next_values", method!(StreamCursor::next_values, 0))?;
    cursor.define_method("close", method!(StreamCursor::close, 0))?;
    let _: Value = seen_module.funcall("private_constant", ("Stream", "Cursor"))?;

    for (name, superclass) in [
        ("InvalidPattern", ruby.exception_regexp_error()),
        ("InvalidOption", ruby.exception_arg_error()),
        ("InvalidType", ruby.exception_type_error()),
        ("OutOfRange", ruby.exception_range_error()),
        ("IOError", ruby.exception_io_error()),
    ] {
        seen_module
            .define_error(name, superclass)?
            .include_module(error)?;
    }

    seen_module.define_singleton_method("native_each_path", function!(seen_each_path, -1))?;
    seen_module.define_singleton_method("native_each_line", function!(seen_each_line, -1))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session() -> StreamSession {
        StreamSession::new().expect("session should initialize")
    }

    #[test]
    fn dropping_a_source_cancels_its_active_session() {
        let source = StreamSource::new(StreamConfig::Search(SearchConfig::default()));
        let session = Arc::new(session());
        source
            .sessions
            .lock()
            .expect("sessions lock should work")
            .push(Arc::downgrade(&session));

        drop(source);

        assert!(session.cancelled.load(Ordering::Relaxed));
    }

    #[test]
    fn cancellation_releases_producers_racing_into_the_wait() {
        const PRODUCERS: usize = 8;
        const TRIALS: usize = 2_000;

        for trial in 0..TRIALS {
            let session = Arc::new(session());
            for index in 0..session.capacity {
                assert!(session.push(StreamEvent::Search(vec![index.to_string().into_bytes()])));
            }

            let producers: Vec<_> = (0..PRODUCERS)
                .map(|_| {
                    let session = Arc::clone(&session);
                    std::thread::spawn(move || {
                        session.push(StreamEvent::Search(vec![b"blocked".to_vec()]))
                    })
                })
                .collect();
            for _ in 0..trial % 64 {
                std::hint::spin_loop();
            }

            session.cancel();

            for producer in producers {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
                while !producer.is_finished() {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "trial {trial}: a producer never woke from `space`"
                    );
                    std::thread::yield_now();
                }
                assert!(!producer.join().expect("producer should not panic"));
            }
        }
    }

    #[test]
    fn ready_events_keep_batch_order() {
        let session = session();
        assert!(session.push(StreamEvent::Search(vec![b"first".to_vec()])));
        assert!(session.push(StreamEvent::Search(vec![b"second".to_vec()])));

        assert!(matches!(
            session.take_ready(),
            Some(StreamNext::Event(StreamEvent::Search(paths))) if paths == [b"first".to_vec()]
        ));
        assert!(matches!(
            session.take_ready(),
            Some(StreamNext::Event(StreamEvent::Search(paths))) if paths == [b"second".to_vec()]
        ));
    }

    #[test]
    fn cancellation_drains_queued_events() {
        let session = session();
        assert!(session.push(StreamEvent::Search(vec![
            b"first".to_vec(),
            b"second".to_vec(),
        ])));
        assert!(session.push(StreamEvent::Search(vec![b"third".to_vec()])));

        drop(session.take_ready());
        session.cancel();

        let state = session.state.lock().expect("state lock should work");
        assert!(state.events.is_empty());
        drop(state);
    }
}
