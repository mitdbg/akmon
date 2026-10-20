//! Entry point for the ingest worker: read one source, emit records.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Instant;

/// One record as the source emitted it, before the graph assigns ids.
#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    pub source_record_id: String,
    pub subtype: String,
    pub attrs: BTreeMap<String, String>,
}

/// Why a run stopped before publishing.
#[derive(Debug)]
pub enum StopReason {
    SourceIncomplete { expected: usize, got: usize },
    PolicyRefused(String),
}

impl fmt::Display for StopReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StopReason::SourceIncomplete { expected, got } => {
                write!(f, "ingest_source_incomplete: {got} of {expected} rows")
            }
            StopReason::PolicyRefused(policy) => write!(f, "policy {policy} refused the read"),
        }
    }
}

/// A run over one source: counts what it read and what it emitted.
pub struct Run {
    started: Instant,
    read: usize,
    emitted: Vec<Record>,
}

impl Run {
    pub fn new() -> Self {
        Run { started: Instant::now(), read: 0, emitted: Vec::new() }
    }

    /// Accept one raw row. Rows without a stable id are counted and dropped,
    /// never given a made-up id: a guessed id would merge two records.
    pub fn accept(&mut self, row: BTreeMap<String, String>) {
        self.read += 1;
        let Some(id) = row.get("id").cloned() else { return };
        let subtype = row.get("kind").cloned().unwrap_or_else(|| "document".to_string());
        self.emitted.push(Record { source_record_id: id, subtype, attrs: row });
    }

    /// Finish the run. An incomplete read is a stop, not a partial publish.
    pub fn finish(self, expected: usize) -> Result<Vec<Record>, StopReason> {
        if self.read < expected {
            return Err(StopReason::SourceIncomplete { expected, got: self.read });
        }
        eprintln!("read {} rows in {:?}", self.read, self.started.elapsed());
        Ok(self.emitted)
    }
}

fn main() {
    let mut run = Run::new();
    for index in 0..3 {
        let mut row = BTreeMap::new();
        row.insert("id".to_string(), format!("doc-{index}"));
        row.insert("kind".to_string(), "document".to_string());
        run.accept(row);
    }
    match run.finish(3) {
        Ok(records) => println!("emitted {} records", records.len()),
        Err(reason) => eprintln!("stopped: {reason}"),
    }
}
