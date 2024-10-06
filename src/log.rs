use std::fmt;
use std::io::{Result as IoResult, Write};
use std::sync::{Arc, Mutex};

use datafusion::common::{DataFusionError, Result as DataFusionResult};

/// Represents a log of query caching operations.
///
/// Logs should be cheap to clone, e.g. they should contain an Arc to the actual log data if applicable.
pub trait AbstractLog: Send + Sync + fmt::Debug + Clone + 'static {
    fn log(&self, level: Level, query_fingerprint: &str, message: impl fmt::Display) -> DataFusionResult<()>;

    fn info(&self, query_fingerprint: &str, message: impl fmt::Display) -> DataFusionResult<()> {
        self.log(Level::Info, query_fingerprint, message)
    }

    fn warn(&self, query_fingerprint: &str, message: impl fmt::Display) -> DataFusionResult<()> {
        self.log(Level::Warn, query_fingerprint, message)
    }

    /// Returns the log of query caching operations if one is recorded.
    fn history(&self) -> Option<Vec<QueryLog>> {
        None
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Level {
    Info,
    Warn,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct QueryLog {
    query_fingerprint: String,
    log: Vec<(Level, String)>,
}

#[derive(Debug, Clone)]
pub struct LogNoOp;

impl AbstractLog for LogNoOp {
    fn log(&self, _level: Level, _query_fingerprint: &str, _message: impl fmt::Display) -> DataFusionResult<()> {
        // Do nothing
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct LogStderrColors {
    last_fingerprint: Arc<Mutex<String>>,
}

impl LogStderrColors {
    fn log_inner(&self, level: Level, query_fingerprint: &str, message: impl fmt::Display) -> IoResult<()> {
        // here so it works with features
        use termcolor::{BufferWriter, Color, ColorChoice, ColorSpec, WriteColor};

        let mut last_fingerprint = self.last_fingerprint.lock().unwrap();

        let buf_writer = BufferWriter::stderr(ColorChoice::Auto);
        let mut buffer = buf_writer.buffer();
        if *last_fingerprint != query_fingerprint {
            *last_fingerprint = query_fingerprint.to_string();
            buffer.set_color(ColorSpec::new().set_fg(Some(Color::Green)))?;
            writeln!(&mut buffer, "{query_fingerprint}")?;
        }

        match level {
            Level::Info => {
                buffer.set_color(ColorSpec::new().set_fg(Some(Color::Blue)))?;
                write!(&mut buffer, "         {message}")?;
            }
            Level::Warn => {
                buffer.set_color(ColorSpec::new().set_fg(Some(Color::Yellow)))?;
                write!(&mut buffer, "  [WARN] {message}")?;
            }
        }
        writeln!(&mut buffer)?;
        buffer.reset()?;
        buf_writer.print(&buffer)
    }
}

impl AbstractLog for LogStderrColors {
    fn log(&self, level: Level, query_fingerprint: &str, message: impl fmt::Display) -> DataFusionResult<()> {
        self.log_inner(level, query_fingerprint, message)
            .map_err(|e| DataFusionError::Internal(format!("Failed to write to stderr: {e}")))
    }
}

macro_rules! log_info {
    ($log:expr, $query_fingerprint:expr, $message:expr) => {
        $log.info($query_fingerprint, $message)?;
    };

    ($log:expr, $query_fingerprint:expr, $message:expr, $( $msg_args:expr ),+ ) => {
        $log.info($query_fingerprint, format!($message, $( $msg_args ),+))?;
    };
}
pub(crate) use log_info;

macro_rules! log_warn {
    ($log:expr, $query_fingerprint:expr, $message:expr) => {
        $log.warn($query_fingerprint, $message)?;
    };

    ($log:expr, $query_fingerprint:expr, $message:expr, $( $msg_args:expr ),+ ) => {
        $log.warn($query_fingerprint, format!($message, $( $msg_args ),+))?;
    };
}
pub(crate) use log_warn;
