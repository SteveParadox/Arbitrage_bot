use std::{
    fs::{self, File, OpenOptions},
    io::{BufWriter, Write},
    path::Path,
};

use anyhow::{Context, Result};

use crate::ArbitrageScanRecord;

pub struct NdjsonRecorder {
    writer: BufWriter<File>,
}

impl NdjsonRecorder {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create scan record directory {}",
                    parent.display()
                )
            })?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("failed to open scan record file {}", path.display()))?;

        Ok(Self {
            writer: BufWriter::new(file),
        })
    }

    pub fn record_batch(&mut self, records: &[ArbitrageScanRecord]) -> Result<()> {
        for record in records {
            serde_json::to_writer(&mut self.writer, record)?;
            self.writer.write_all(b"\n")?;
        }
        self.writer.flush()?;
        Ok(())
    }
}
