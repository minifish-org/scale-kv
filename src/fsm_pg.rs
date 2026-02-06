use crate::{Error, PAGE_SIZE, Page, PageId, Result};

/// PG-style Free Space Map (FSM) stored in pages.
///
/// We model free space as a u8 "class" (0..=255). Each data page has a class.
/// Higher levels store max(class) over fixed-size groups.
///
/// Layout:
/// - Meta page: fixed id `FSM_META_PAGE_ID`
///   - magic + version
///   - data_base (u64)
///   - leaf_count (u64)  (# data pages tracked)
///   - level_count (u32)
///   - level0_base_page_id (u64)  (where level pages start)
///   - fanout (u32) = bytes per page = 16384 (for u8 entries)
///
/// Levels are stored as a dense array of u8 entries, packed into pages.
/// Level 0 has one entry per data page index.
/// Level k+1 has one entry per group of FANOUT entries in level k.
///
/// Each level is stored starting at a computed page id.

pub const FSM_META_PAGE_ID: PageId = 1;

const MAGIC: &[u8; 8] = b"SKFSM\0\0\0";
const VERSION: u32 = 1;

const FANOUT: usize = PAGE_SIZE; // entries per page for u8 array

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FsmMeta {
    pub data_base: PageId,
    pub leaf_count: u64,
    pub level0_base: PageId,
    pub level_count: u32,
}

impl FsmMeta {
    pub fn encode(&self) -> Page {
        let mut p = vec![0u8; PAGE_SIZE];
        p[0..8].copy_from_slice(MAGIC);
        p[8..12].copy_from_slice(&VERSION.to_le_bytes());
        p[12..20].copy_from_slice(&self.data_base.to_le_bytes());
        p[20..28].copy_from_slice(&self.leaf_count.to_le_bytes());
        p[28..32].copy_from_slice(&self.level_count.to_le_bytes());
        p[32..40].copy_from_slice(&self.level0_base.to_le_bytes());
        p
    }

    pub fn decode(page: &[u8]) -> Result<Self> {
        if page.len() != PAGE_SIZE {
            return Err(Error::InvalidPageSize(page.len(), PAGE_SIZE));
        }
        if &page[0..8] != MAGIC {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid fsm meta magic",
            )));
        }
        let ver = u32::from_le_bytes(page[8..12].try_into().unwrap());
        if ver != VERSION {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unsupported fsm meta version: {ver}"),
            )));
        }
        let data_base = u64::from_le_bytes(page[12..20].try_into().unwrap());
        let leaf_count = u64::from_le_bytes(page[20..28].try_into().unwrap());
        let level_count = u32::from_le_bytes(page[28..32].try_into().unwrap());
        let level0_base = u64::from_le_bytes(page[32..40].try_into().unwrap());
        Ok(Self {
            data_base,
            leaf_count,
            level0_base,
            level_count,
        })
    }
}

fn levels_for_leaves(mut leaves: u64) -> u32 {
    // Each level groups FANOUT entries into 1.
    let mut levels = 1u32;
    while leaves > 1 {
        leaves = ((leaves as usize + FANOUT - 1) / FANOUT) as u64;
        levels += 1;
    }
    levels
}

fn level_len(leaves: u64, level: u32) -> u64 {
    let mut n = leaves;
    for _ in 0..level {
        n = ((n as usize + FANOUT - 1) / FANOUT) as u64;
    }
    n
}

fn level_page_count(leaves: u64, level: u32) -> u64 {
    let n = level_len(leaves, level);
    ((n as usize + FANOUT - 1) / FANOUT) as u64
}

fn level_base(meta: &FsmMeta, level: u32) -> PageId {
    let mut base = meta.level0_base;
    for l in 0..level {
        base += level_page_count(meta.leaf_count, l) as u64;
    }
    base
}

fn entry_loc(meta: &FsmMeta, level: u32, idx: u64) -> (PageId, usize) {
    let base = level_base(meta, level);
    let page_off = (idx as usize) / FANOUT;
    let in_page = (idx as usize) % FANOUT;
    (base + page_off as u64, in_page)
}

pub fn class_from_free_bytes(free: usize) -> u8 {
    // 64B granularity.
    let c = free / 64;
    c.min(255) as u8
}

pub fn need_class(need_bytes: usize) -> u8 {
    class_from_free_bytes(need_bytes)
}

/// Initialize a fresh FSM meta and empty level pages.
///
/// Returns list of page writes (meta + level pages) to be included in the creating txn.
pub fn init_fsm_pages(
    data_base: PageId,
    leaves: u64,
    level0_base: PageId,
) -> (FsmMeta, Vec<(PageId, Page)>) {
    let level_count = levels_for_leaves(leaves);
    let meta = FsmMeta {
        data_base,
        leaf_count: leaves,
        level0_base,
        level_count,
    };

    let mut writes: Vec<(PageId, Page)> = Vec::new();
    writes.push((FSM_META_PAGE_ID, meta.encode()));

    // Allocate all level pages as zero.
    let mut pid = level0_base;
    for l in 0..level_count {
        let pages = level_page_count(leaves, l);
        for _ in 0..pages {
            writes.push((pid, vec![0u8; PAGE_SIZE]));
            pid += 1;
        }
    }

    (meta, writes)
}

/// Read a u8 entry from a level page.
pub fn get_entry(
    meta: &FsmMeta,
    pages: &impl Fn(PageId) -> Option<Page>,
    level: u32,
    idx: u64,
) -> Result<u8> {
    let (pid, off) = entry_loc(meta, level, idx);
    let p = pages(pid).ok_or(Error::InMemoryPageMissing(pid))?;
    Ok(*p.get(off).unwrap_or(&0))
}

/// Set a u8 entry in a level page (returns updated page after-image).
pub fn set_entry(
    meta: &FsmMeta,
    pages: &impl Fn(PageId) -> Option<Page>,
    level: u32,
    idx: u64,
    value: u8,
) -> Result<(PageId, Page)> {
    let (pid, off) = entry_loc(meta, level, idx);
    let mut p = pages(pid).unwrap_or_else(|| vec![0u8; PAGE_SIZE]);
    p[off] = value;
    Ok((pid, p))
}

/// Find a candidate data page index whose class >= need_class.
/// Returns None if not found.
pub fn find_candidate(
    meta: &FsmMeta,
    pages: &impl Fn(PageId) -> Option<Page>,
    need: u8,
) -> Result<Option<u64>> {
    // Start from top level.
    let top = meta.level_count.saturating_sub(1);
    // If root max < need, none.
    let root_max = get_entry(meta, pages, top, 0)?;
    if root_max < need {
        return Ok(None);
    }

    let mut idx = 0u64;
    for level in (0..top).rev() {
        // idx at parent level corresponds to group [idx*FANOUT .. idx*FANOUT+FANOUT)
        let start = idx * FANOUT as u64;
        let end = start + FANOUT as u64;
        let mut found = None;
        let limit = level_len(meta.leaf_count, level).min(end);
        for child in start..limit {
            let v = get_entry(meta, pages, level, child)?;
            if v >= need {
                found = Some(child);
                break;
            }
        }
        match found {
            Some(c) => idx = c,
            None => return Ok(None),
        }
    }

    // idx is a leaf index
    if idx >= meta.leaf_count {
        return Ok(None);
    }
    Ok(Some(idx))
}

/// Update a leaf class and fix up parent max values along the path.
///
/// Returns a list of page after-images that should be written.
pub fn update_leaf(
    meta: &FsmMeta,
    pages: &impl Fn(PageId) -> Option<Page>,
    leaf_idx: u64,
    new_class: u8,
) -> Result<Vec<(PageId, Page)>> {
    let mut writes: Vec<(PageId, Page)> = Vec::new();

    // Level 0 leaf.
    let (pid0, mut p0) = set_entry(meta, pages, 0, leaf_idx, new_class)?;
    writes.push((pid0, p0.clone()));

    // Recompute parents.
    let mut child_idx = leaf_idx;
    for level in 1..meta.level_count {
        let parent_idx = child_idx / FANOUT as u64;
        let start = parent_idx * FANOUT as u64;
        let end = start + FANOUT as u64;
        let limit = level_len(meta.leaf_count, level - 1).min(end);
        let mut maxv = 0u8;
        for i in start..limit {
            let v = get_entry(meta, pages, level - 1, i)?;
            if v > maxv {
                maxv = v;
            }
        }
        let (pid, p) = set_entry(meta, pages, level, parent_idx, maxv)?;
        writes.push((pid, p));
        child_idx = parent_idx;
    }

    Ok(writes)
}
