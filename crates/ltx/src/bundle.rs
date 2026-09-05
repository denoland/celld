//! The bundle envelope: a container of verbatim L0 LTX files, never a
//! format.
//!
//! celld-original, not a Litestream port. Layout: the concatenated raw L0
//! bytes, a binary footer of rows, the footer length as a little-endian
//! u32, and a four-byte magic. A row is `u16 cell_len | cell utf-8 |
//! u64 cell_epoch | u64 txid | u64 offset | u64 len`, all little-endian.
//! Un-bundling is arithmetic. The inner bytes are exactly what the
//! per-cell writer produces, so byte-level compatibility stays checkable.
//! At rest, the bucket is pure Litestream. A drained bundle leaves no trace
//! that this module ever existed. Nothing else in this crate depends on bundles;
//! deleting this file returns the crate to its upstream shape.

use crate::error::{Error, Result};
use crate::TXID;

const MAGIC: &[u8; 4] = b"CLB1";

fn malformed(what: &str) -> Error {
    Error::Other(format!("bundle: {what}").into())
}

fn split_data_and_footer(bundle: &[u8]) -> Result<(&[u8], &[u8])> {
    if bundle.len() < 8 {
        return Err(malformed("shorter than its trailer"));
    }
    let (rest, magic) = bundle.split_at(bundle.len() - 4);
    if magic != MAGIC {
        return Err(malformed("bad magic"));
    }
    let (rest, len_bytes) = rest.split_at(rest.len() - 4);
    let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
    if rest.len() < len {
        return Err(malformed("footer overruns the object"));
    }
    Ok(rest.split_at(rest.len() - len))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BundleRow {
    pub cell: String,
    pub cell_epoch: u64,
    pub txid: u64,
    pub offset: u64,
    pub len: u64,
}

impl BundleRow {
    pub fn txid(&self) -> TXID {
        TXID(self.txid)
    }
}

/// One captured L0 segment headed for a bundle.
pub struct BundleEntry {
    pub cell: String,
    pub cell_epoch: u64,
    pub txid: u64,
    pub bytes: Vec<u8>,
}

pub fn encode(entries: &[BundleEntry]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut footer = Vec::new();
    for entry in entries {
        let cell = entry.cell.as_bytes();
        let cell_len: u16 = cell
            .len()
            .try_into()
            .map_err(|_| malformed("cell name too long"))?;
        footer.extend_from_slice(&cell_len.to_le_bytes());
        footer.extend_from_slice(cell);
        for value in [
            entry.cell_epoch,
            entry.txid,
            out.len() as u64,
            entry.bytes.len() as u64,
        ] {
            footer.extend_from_slice(&value.to_le_bytes());
        }
        out.extend_from_slice(&entry.bytes);
    }
    let len: u32 = footer
        .len()
        .try_into()
        .map_err(|_| malformed("footer too large"))?;
    out.extend_from_slice(&footer);
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(MAGIC);
    Ok(out)
}

/// The rows, from a complete bundle object.
pub fn decode_rows(bundle: &[u8]) -> Result<Vec<BundleRow>> {
    let (_, mut footer) = split_data_and_footer(bundle)?;
    let mut rows = Vec::new();
    while !footer.is_empty() {
        if footer.len() < 2 {
            return Err(malformed("truncated row"));
        }
        let cell_len = u16::from_le_bytes(footer[..2].try_into().unwrap()) as usize;
        footer = &footer[2..];
        if footer.len() < cell_len + 32 {
            return Err(malformed("truncated row"));
        }
        let cell = std::str::from_utf8(&footer[..cell_len])
            .map_err(|_| malformed("cell name not utf-8"))?
            .to_string();
        footer = &footer[cell_len..];
        let mut values = [0_u64; 4];
        for value in &mut values {
            *value = u64::from_le_bytes(footer[..8].try_into().unwrap());
            footer = &footer[8..];
        }
        rows.push(BundleRow {
            cell,
            cell_epoch: values[0],
            txid: values[1],
            offset: values[2],
            len: values[3],
        });
    }
    Ok(rows)
}

/// One row's verbatim L0 bytes out of a complete bundle object.
pub fn slice<'a>(bundle: &'a [u8], row: &BundleRow) -> Result<&'a [u8]> {
    let (data, _) = split_data_and_footer(bundle)?;
    let start = usize::try_from(row.offset).map_err(|_| malformed("row overflows"))?;
    let len = usize::try_from(row.len).map_err(|_| malformed("row overflows"))?;
    let end = start
        .checked_add(len)
        .ok_or_else(|| malformed("row overflows"))?;
    if end > data.len() {
        return Err(malformed("row overruns the data section"));
    }
    Ok(&data[start..end])
}
