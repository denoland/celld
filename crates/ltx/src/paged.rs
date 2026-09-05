//! paged.rs — read-on-demand paging from replica objects.
//!
//! The read path of paged restore, ported from Litestream's
//! VFS. A cold cell serves a page by fetching only that page's frame from the
//! bucket instead of downloading its whole restore chain: build a page map from
//! each planned object's trailing page index, then range-read and decode one
//! frame per demanded page.
//!
//! An LTX file ends with `[page index][8-byte index length][16-byte trailer]`,
//! and every object celld writes carries the index (`codec::Encoder::close`),
//! so the map reads the index tail-first and never downloads the frames it does
//! not need.

use crate::client::ReplicaClient;
use crate::codec::{decode_page_frame_of, decode_page_index};
use crate::error::{Error, Result};
use crate::ltx::{FileInfo, Header, HEADER_SIZE, TRAILER_SIZE};
use crate::ltx_file_path;
use crate::TXID;
use futures_util::StreamExt;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::io::Read;
use std::io::Seek;
use std::sync::Mutex;

/// Bytes fetched from the object tail first, sized to hold the whole page index
/// of an ordinary object in one read; a larger index needs a second read. A
/// fold of thirty 1 MB rows indexes ~8,000 pages in ~70 KiB, so 32 KiB cost
/// every object of the 5 GB whale a second round trip: its map took 12 s on
/// the node. Bytes are cheap on a ranged read; rounds are the cost.
const TAIL_FETCH_BYTES: u64 = 256 * 1024;

/// Width of the big-endian page-index-length field, between the index and the
/// trailer.
const INDEX_LEN_FIELD: u64 = 8;

/// The most frame bytes one fault fetches. A fault brings its page's whole
/// run — the consecutive pages stored as adjacent frames of the same object,
/// which is how a snapshot or compaction lays a b-tree's leaves and a blob's
/// overflow chain out — so a sequential read costs one round-trip per run
/// instead of one per 4KiB page. 64 pages caps the waste on a scattered
/// point lookup, whose interior pages sit in runs it will not read.
pub const READAHEAD_BYTES: u64 = 1024 * 1024;

/// Object page indexes fetched at once while building a map: a whale's plan
/// of ~200 objects in a few rounds.
const INDEX_FETCH_CONCURRENCY: usize = 32;

/// Fetches and decodes one object's page index with ranged reads, without
/// downloading the object. It reads the tail, takes the index length from the
/// 8-byte field before the trailer, and decodes the index — a second exact
/// read only when the index is larger than the tail fetch.
///
/// Returns the `(pgno, frame_offset, frame_size)` entries: each page's whole
/// frame lives at `frame_offset` in the file, `frame_size` bytes long.
pub async fn fetch_page_index<C: ReplicaClient>(
    client: &C,
    info: &FileInfo,
) -> Result<Vec<(u32, u64, u64)>> {
    let size = u64::try_from(info.size).map_err(|_| Error::LTXCorrupted)?;
    let footer = INDEX_LEN_FIELD + TRAILER_SIZE as u64;
    if size < footer {
        return Err(Error::LTXCorrupted);
    }

    let tail_len = TAIL_FETCH_BYTES.min(size);
    let tail_start = size - tail_len;
    let tail = client
        .read_range(
            info.level,
            info.min_txid,
            info.max_txid,
            tail_start,
            tail_len,
        )
        .await?;
    if (tail.len() as u64) < footer {
        return Err(Error::LTXCorrupted);
    }

    // The index-length field sits just before the trailer; the index precedes
    // it. Compute the index's file range from the object size, so a partial
    // tail read still locates it.
    let field_at = tail.len() - TRAILER_SIZE - INDEX_LEN_FIELD as usize;
    let index_len = u64::from_be_bytes(
        tail[field_at..field_at + INDEX_LEN_FIELD as usize]
            .try_into()
            .map_err(|_| Error::LTXCorrupted)?,
    );
    let index_end = size - footer;
    let index_start = index_end
        .checked_sub(index_len)
        .ok_or(Error::LTXCorrupted)?;

    if index_start >= tail_start {
        let lo = (index_start - tail_start) as usize;
        let hi = (index_end - tail_start) as usize;
        decode_page_index(&tail[lo..hi])
    } else {
        let index_bytes = client
            .read_range(
                info.level,
                info.min_txid,
                info.max_txid,
                index_start,
                index_len,
            )
            .await?;
        decode_page_index(&index_bytes)
    }
}

/// Where one page's frame lives: the object that holds it and the byte range of
/// its frame within that object.
#[derive(Debug, Clone)]
pub struct PageLocator {
    pub info: FileInfo,
    pub offset: u64,
    pub size: u64,
}

/// An immutable per-activation view of a restore cut: which object serves each
/// page, plus the page size and the database's page count at the cut. Built
/// once from a plan and never updated in place, so a faulted page always comes
/// from the pinned cut even as newer objects land.
#[derive(Debug)]
pub struct PageMap {
    pub page_size: u32,
    pub commit: u32,
    pub pages: HashMap<u32, PageLocator>,
}

/// Builds a page map over a restore `plan` (as [`crate::replica::calc_restore_plan`]
/// returns it, oldest object first) without downloading the objects. It fetches
/// each object's page index and inserts its pages, so a later object in the
/// chain overrides an earlier one — last-writer-wins, the newest version of
/// each page. The page size and commit come from the newest object's header.
pub async fn build_page_map<C: ReplicaClient>(client: &C, plan: &[FileInfo]) -> Result<PageMap> {
    let newest = plan.last().ok_or(Error::TxNotAvailable)?;
    let mut pages: HashMap<u32, PageLocator> = HashMap::new();
    // The indexes are independent reads, so fetch them concurrently; a whale's
    // plan of two dozen objects otherwise costs two round-trips each, in
    // series, on the activation path. `buffered` keeps plan order for the
    // last-writer-wins insertion below.
    // Built as a Vec first: a closure held inside this future's state would
    // make it higher-ranked over the plan's borrow and unspawnable.
    let header_bytes = client
        .read_range(
            newest.level,
            newest.min_txid,
            newest.max_txid,
            0,
            HEADER_SIZE as u64,
        )
        .await?;
    let header = Header::parse(&header_bytes)?;
    let fetches: Vec<_> = plan
        .iter()
        .map(|info| fetch_page_index(client, info))
        .collect();
    let mut indexes = futures_util::stream::iter(fetches).buffered(INDEX_FETCH_CONCURRENCY);
    let mut planned = plan.iter();
    while let Some(index) = indexes.next().await {
        let info = planned.next().expect("one index per planned object");
        let index = index?;
        // An older object can hold pages past the newest commit (the
        // database shrank since); they are not part of the cut.
        for (pgno, offset, size) in index.into_iter().filter(|(p, _, _)| *p <= header.commit) {
            pages.insert(
                pgno,
                PageLocator {
                    info: info.clone(),
                    offset,
                    size,
                },
            );
        }
    }
    Ok(PageMap {
        page_size: header.page_size,
        commit: header.commit,
        pages,
    })
}

impl PageMap {
    /// Serves one page: range-reads its frame from the object the map points to
    /// and decodes it. `None` when the cut has no such page (a page above the
    /// commit, or one never written).
    pub async fn read_page<C: ReplicaClient>(
        &self,
        client: &C,
        pgno: u32,
    ) -> Result<Option<Vec<u8>>> {
        let Some(loc) = self.pages.get(&pgno) else {
            return Ok(None);
        };
        let frame = client
            .read_range(
                loc.info.level,
                loc.info.min_txid,
                loc.info.max_txid,
                loc.offset,
                loc.size,
            )
            .await?;
        Ok(Some(decode_page_frame_of(
            &frame,
            self.page_size as usize,
            pgno,
        )?))
    }

    /// The VFS fault path, on whatever thread SQLite called `xRead` from:
    /// `pgno`'s page plus its run (see [`READAHEAD_BYTES`]), in one ranged
    /// read of the object that holds them. Empty when the cut has no such
    /// page; otherwise the first entry is `pgno`.
    pub fn read_run_sync(
        &self,
        reader: &dyn RangeReader,
        pgno: u32,
        max_bytes: u64,
    ) -> Result<Vec<(u32, Vec<u8>)>> {
        let Some(first) = self.pages.get(&pgno) else {
            return Ok(Vec::new());
        };
        // The cap is in decoded page bytes: frames are LZ4-compressed, and a
        // cap on frame bytes let one fault of a compressible file bring the
        // whole database (a 13MB fixture in a single 1MiB "run").
        let max_pages = (max_bytes / u64::from(self.page_size)).max(1) as u32;
        let mut end = first.offset + first.size;
        let mut last = pgno;
        while let Some(next) = self.pages.get(&(last + 1)) {
            let adjacent = next.info == first.info && next.offset == end;
            if !adjacent || last + 1 - pgno >= max_pages {
                break;
            }
            end += next.size;
            last += 1;
        }
        let bytes = reader.read_range(&first.info, first.offset, end - first.offset)?;
        let mut run = Vec::with_capacity((last - pgno + 1) as usize);
        let mut at = 0;
        if (bytes.len() as u64) < end - first.offset {
            return Err(Error::LTXCorrupted);
        }
        for pgno_ in pgno..=last {
            let size = self.pages[&pgno_].size as usize;
            let page = decode_page_frame_of(&bytes[at..at + size], self.page_size as usize, pgno_)?;
            run.push((pgno_, page));
            at += size;
        }
        Ok(run)
    }
}

/// A blocking ranged read of one replica object: the fault path's I/O.
///
/// SQLite's `xRead` is synchronous and runs on whichever thread entered the
/// connection — a blocking thread for the managed Db, but a tokio task's
/// thread for the actor's own queries, since isolates are entered from tasks.
/// Every bridge from that thread into the async client failed in one context
/// (`block_on` on a worker, `block_in_place` on a current-thread runtime, a
/// same-runtime spawn stuck in the worker's LIFO slot). So the fault path does
/// its own I/O on the calling thread with a plain socket and never touches a
/// runtime, which is correct from any thread by construction.
pub trait RangeReader: Send + Sync {
    /// `len` bytes of `info`'s object starting at `offset`.
    fn read_range(&self, info: &FileInfo, offset: u64, len: u64) -> Result<Vec<u8>>;
}

/// A [`RangeReader`] over the file replica layout (`ltx_file_path`).
pub struct FileRangeReader {
    root: String,
}

impl FileRangeReader {
    pub fn new(root: impl Into<String>) -> Self {
        Self { root: root.into() }
    }
}

// The file replica is a development and test backend, not node storage:
// it reads the same directory the file client writes with tokio::fs, outside
// the injected node filesystem the lint guards.
#[allow(clippy::disallowed_methods)]
impl RangeReader for FileRangeReader {
    fn read_range(&self, info: &FileInfo, offset: u64, len: u64) -> Result<Vec<u8>> {
        let path = ltx_file_path(&self.root, info.level as u32, info.min_txid, info.max_txid);
        let mut file = std::fs::File::open(path)?;
        file.seek(std::io::SeekFrom::Start(offset))?;
        let mut out = vec![0; len as usize];
        file.read_exact(&mut out)?;
        Ok(out)
    }
}

/// Children a fault should bring along, read off the faulted page's b-tree
/// structure. Round-trips are the whole cost of a fault (a 4KiB and a 1MiB
/// range cost about the same), and SQLite asks for pages one at a time, so
/// the only way below one round-trip per page is to read the structure: an
/// interior page names every child. Fetching the children concurrently makes
/// a scan cost one round per tree level instead of one per leaf. A misparse
/// can only waste bytes: every served page still comes from the map.
///
/// A leaf's overflow chains are deliberately not here: a chain is stored as
/// a run, so the sequential walk SQLite does gets it in one read after the
/// first overflow fault, while fetching every cell's chain at the leaf fault
/// pulls the neighbours' megabytes for a single-row read.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Prefetch {
    /// Child pages of an interior page, in cell order.
    pub children: Vec<u32>,
}

/// Children fetched ahead of a fault on a child page, along the tree in
/// page order: a scan then costs one round per window of leaves, and a
/// point read pays for one window at most.
const SCAN_AHEAD: usize = 64;

/// Concurrent range reads while fetching ahead.
const PREFETCH_WORKERS: usize = 64;

/// Parses a decoded b-tree page for what to prefetch; page 1 carries the
/// 100-byte file header. Anything malformed yields an empty plan.
pub fn prefetch_plan(pgno: u32, page: &[u8]) -> Prefetch {
    let mut plan = Prefetch::default();
    let base = if pgno == 1 { 100 } else { 0 };
    let Some(head) = page.get(base..base + 12) else {
        return plan;
    };
    // 0x05 interior table, 0x02 interior index; leaves have no children.
    if head[0] != 0x05 && head[0] != 0x02 {
        return plan;
    }
    let cells = u16::from_be_bytes([head[3], head[4]]) as usize;
    let ptrs = base + 12;
    for i in 0..cells {
        let Some(at) = page.get(ptrs + 2 * i..ptrs + 2 * i + 2) else {
            break;
        };
        let at = u16::from_be_bytes([at[0], at[1]]) as usize;
        let Some(child) = page.get(at..at + 4) else {
            break;
        };
        plan.children
            .push(u32::from_be_bytes([child[0], child[1], child[2], child[3]]));
    }
    plan.children
        .push(u32::from_be_bytes([head[8], head[9], head[10], head[11]]));
    plan
}

/// A synchronous page reader over a [`PageMap`] for the paged SQLite VFS:
/// the pinned cut's map plus a blocking [`RangeReader`] for its objects.
///
/// celld's VFS faults a missing page into a local sparse main-db file, so
/// SQLite then runs entirely normally over that file (WAL, page-1 writes,
/// checkpoint) and the local file is the cache. This source therefore holds no
/// cache and does not rewrite page 1: it just serves the pinned cut's bytes for
/// a page the local file does not have yet.
pub struct PageSource {
    map: PageMap,
    reader: Box<dyn RangeReader>,
    /// B-tree child pages named by an interior page this source has decoded
    /// and not yet delivered, and the last one that faulted: a fault on one
    /// fetches it alone, and a fault that continues along the tree looks
    /// ahead (see [`PageSource::read_run_blocking`]).
    scan: Mutex<Scan>,
    /// Faults served: every `read_run_blocking`, foreground or hydration.
    faults: std::sync::atomic::AtomicU64,
}

#[derive(Default)]
struct Scan {
    children: BTreeSet<u32>,
    last: Option<u32>,
}

impl PageSource {
    pub fn new(map: PageMap, reader: Box<dyn RangeReader>) -> Self {
        Self {
            map,
            reader,
            scan: Mutex::new(Scan::default()),
            faults: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Faults served so far. A hydrated cell's reads leave it unchanged.
    pub fn faults(&self) -> u64 {
        self.faults.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The database page size at the pinned cut.
    pub fn page_size(&self) -> u32 {
        self.map.page_size
    }

    /// The database's page count at the pinned cut. The VFS sizes the local file
    /// to `page_size * commit`.
    pub fn commit(&self) -> u32 {
        self.map.commit
    }

    /// Reads `pgno`'s run on the calling thread ([`PageMap::read_run_sync`]),
    /// then what the faulted page's structure says comes next
    /// ([`prefetch_plan`]), concurrently: every page returned is hydrated by
    /// the VFS, so a scan or a big-row read pays one round-trip per level.
    pub fn read_run_blocking(&self, pgno: u32) -> Result<Vec<(u32, Vec<u8>)>> {
        self.faults
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // A page an interior page named is a b-tree child. Its run is its
        // neighbours in page order — for a leaf of a big-row table, the rows'
        // overflow chains, megabytes a scan never reads (the whale's
        // count(*) faulted 700 MB). Fetch it alone. When the fault continues
        // along the tree (the last child fault was the nearest one below),
        // it is a scan: bring the next children with it, a window at a time.
        // A point read walks one path, so it never looks ahead: its cost
        // stays one request per level.
        let ahead = {
            let mut scan = self.scan.lock().unwrap();
            scan.children.remove(&pgno).then(|| {
                let sequential = scan.last.is_some_and(|last| {
                    last < pgno && scan.children.range(last + 1..pgno).next().is_none()
                });
                scan.last = Some(pgno);
                let next: Vec<u32> = if sequential {
                    scan.children
                        .range(pgno + 1..)
                        .take(SCAN_AHEAD)
                        .copied()
                        .collect()
                } else {
                    Vec::new()
                };
                for p in &next {
                    scan.children.remove(p);
                }
                next
            })
        };
        let page = u64::from(self.map.page_size);
        let mut run = self.map.read_run_sync(
            &*self.reader,
            pgno,
            if ahead.is_some() {
                page
            } else {
                READAHEAD_BYTES
            },
        )?;
        if run.is_empty() {
            return Ok(run);
        }
        if let Some(wanted) = ahead.filter(|w| !w.is_empty()) {
            let fetched = std::thread::scope(|scope| {
                let workers: Vec<_> = wanted
                    .chunks(wanted.len().div_ceil(PREFETCH_WORKERS).max(1))
                    .map(|chunk| {
                        scope.spawn(move || {
                            // Consecutive children (a small-row table's
                            // leaves) come as one run; the rest one by one.
                            let mut pages = Vec::new();
                            let mut i = 0;
                            while i < chunk.len() {
                                let first = chunk[i];
                                let mut len = 1;
                                while i + len < chunk.len() && chunk[i + len] == first + len as u32
                                {
                                    len += 1;
                                }
                                let got = self
                                    .map
                                    .read_run_sync(&*self.reader, first, len as u64 * page)
                                    .unwrap_or_default();
                                i += got.len().max(1);
                                pages.extend(got);
                            }
                            pages
                        })
                    })
                    .collect();
                workers
                    .into_iter()
                    .filter_map(|w| w.join().ok())
                    .flatten()
                    .collect::<Vec<_>>()
            });
            // A child a failed fetch did not deliver goes back on the list;
            // dropped, its own fault would come as a run of its rows.
            let delivered: std::collections::HashSet<u32> =
                fetched.iter().map(|(p, _)| *p).collect();
            let mut scan = self.scan.lock().unwrap();
            scan.children
                .extend(wanted.iter().copied().filter(|p| !delivered.contains(p)));
            drop(scan);
            run.extend(fetched);
        }
        // Every page delivered is off the list; every interior page among
        // them names the children the next faults fetch alone and ahead.
        let mut scan = self.scan.lock().unwrap();
        for (p, page) in &run {
            scan.children.remove(p);
            scan.children.extend(prefetch_plan(*p, page).children);
        }
        Ok(run)
    }

    /// Reads one page on the calling thread. `None` when the cut has no such
    /// page.
    pub fn read_page_blocking(&self, pgno: u32) -> Result<Option<Vec<u8>>> {
        Ok(self
            .read_run_blocking(pgno)?
            .into_iter()
            .next()
            .map(|(_, page)| page))
    }
}

/// Routes a fault to the epoch that holds the object, by the object's first
/// txid, as [`crate::client::epochs::EpochChain`] routes the async reads.
pub struct EpochChainReader {
    spans: Vec<(TXID, Box<dyn RangeReader>)>,
}

impl EpochChainReader {
    /// `spans` ascending by the first txid each reader serves.
    pub fn new(spans: Vec<(TXID, Box<dyn RangeReader>)>) -> Self {
        assert!(!spans.is_empty(), "an epoch chain has a span");
        Self { spans }
    }
}

impl RangeReader for EpochChainReader {
    fn read_range(&self, info: &FileInfo, offset: u64, len: u64) -> Result<Vec<u8>> {
        let (_, reader) = self
            .spans
            .iter()
            .rev()
            .find(|(lo, _)| *lo <= info.min_txid)
            .unwrap_or(&self.spans[0]);
        reader.read_range(info, offset, len)
    }
}
