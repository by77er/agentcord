//! Append-only JSONL files: the durable backing for every log, event and transcript.

use anyhow::{Context, Result};
use serde::{Serialize, de::DeserializeOwned};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

pub struct Jsonl {
    file: File,
    path: PathBuf,
    /// fsync after every record. On for logs and broker events, off for raw pi streams.
    sync: bool,
}

impl Jsonl {
    pub fn open(path: &Path, sync: bool) -> Result<Self> {
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("opening {}", path.display()))?;
        Ok(Self {
            file,
            path: path.to_owned(),
            sync,
        })
    }

    pub fn append<T: Serialize>(&mut self, value: &T) -> Result<()> {
        let mut line = serde_json::to_vec(value)?;
        line.push(b'\n');
        self.write(&line)
    }

    pub fn append_raw(&mut self, line: &str) -> Result<()> {
        let mut buf = Vec::with_capacity(line.len() + 1);
        buf.extend_from_slice(line.as_bytes());
        buf.push(b'\n');
        self.write(&buf)
    }

    fn write(&mut self, buf: &[u8]) -> Result<()> {
        self.file
            .write_all(buf)
            .with_context(|| format!("writing {}", self.path.display()))?;
        if self.sync {
            self.file.sync_data()?;
        }
        Ok(())
    }
}

/// Read every record of a JSONL file. A torn final line (crash mid-write) is skipped.
pub fn read_all<T: DeserializeOwned>(path: &Path) -> Result<Vec<T>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let reader = BufReader::new(File::open(path)?);
    let mut out = Vec::new();
    for (i, line) in reader.lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str(&line) {
            Ok(v) => out.push(v),
            Err(e) => eprintln!(
                "agentcord: skipping bad record {}:{}: {e}",
                path.display(),
                i + 1
            ),
        }
    }
    Ok(out)
}

pub fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, serde_json::to_vec_pretty(value)?)?;
    fs::rename(&tmp, path)?;
    Ok(())
}

pub fn now() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}
