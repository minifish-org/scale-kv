use crate::{Error, PAGE_SIZE, Page, PageId, Result};

/// PG-style Free Space Map (FSM) stored in pages.
///
/// We model free space as a u8 "class" (0..=255). Each data page has a class.
/// Higher levels store max(class) over fixed-size groups.
///
/// This FSM is a hint structure: candidates must be validated against the real data page.
///
/// Meta page (fixed id `FSM_META_PAGE_ID`) stores layout and allocation cursor.

pub const FSM_META_PAGE_ID: PageId = 1;

const MAGIC: &[u8; 8] = b"SKFSM\0\0\0";
const VERSION: u32 = 2;

const FANOUT: usize = PAGE_SIZE; // entries per page for u8 array
const MAX_LEVELS: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FsmMeta {
    pub data_base: PageId,
    pub leaf_count: u64,
    pub level_count: u32,

    /// Base page id for each level (0..level_count).
    pub level_base: [PageId; MAX_LEVELS],

    /// Next free page id for allocating more FSM pages.
    pub next_fsm_page_id: PageId,
}

impl FsmMeta {
    pub fn encode(&self) -> Page {
        let mut p = vec![0u8; PAGE_SIZE];
        p[0..8].copy_from_slice(MAGIC);
        p[8..12].copy_from_slice(&VERSION.to_le_bytes());
        p[12..20].copy_from_slice(&self.data_base.to_le_bytes());
        p[20..28].copy_from_slice(&self.leaf_count.to_le_bytes());
        p[28..32].copy_from_slice(&self.level_count.to_le_bytes());
        let mut off = 32;
        for b in self.level_base {
            p[off..off + 8].copy_from_slice(&b.to_le_bytes());
            off += 8;
        }
        p[off..off + 8].copy_from_slice(&self.next_fsm_page_id.to_le_bytes());
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
        let mut level_base = [0u64; MAX_LEVELS];
        let mut off = 32;
        for i in 0..MAX_LEVELS {
            level_base[i] = u64::from_le_bytes(page[off..off + 8].try_into().unwrap());
            off += 8;
        }
        let next_fsm_page_id = u64::from_le_bytes(page[off..off + 8].try_into().unwrap());

        Ok(Self {
            data_base,
            leaf_count,
            level_count,
            level_base,
            next_fsm_page_id,
        })
    }
}

fn levels_for_leaves(mut leaves: u64) -> u32 {
    let mut levels = 1u32;
    while leaves > 1 {
        leaves = ((leaves as usize + FANOUT - 1) / FANOUT) as u64;
        levels += 1;
        if levels as usize >= MAX_LEVELS {
            break;
        }
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

fn entry_loc(meta: &FsmMeta, level: u32, idx: u64) -> (PageId, usize) {
    let base = meta.level_base[level as usize];
    let page_off = (idx as usize) / FANOUT;
    let in_page = (idx as usize) % FANOUT;
    (base + page_off as u64, in_page)
}

pub fn class_from_free_bytes(free: usize) -> u8 {
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
    let level_count = levels_for_leaves(leaves).min(MAX_LEVELS as u32);

    let mut level_base = [0u64; MAX_LEVELS];
    level_base[0] = level0_base;
    let mut next = level0_base;
    for l in 0..level_count {
        let pages = level_page_count(leaves, l);
        next += pages;
        if (l as usize) + 1 < MAX_LEVELS {
            level_base[(l as usize) + 1] = next;
        }
    }

    let meta = FsmMeta {
        data_base,
        leaf_count: leaves,
        level_count,
        level_base,
        next_fsm_page_id: next,
    };

    let mut writes: Vec<(PageId, Page)> = Vec::new();
    writes.push((FSM_META_PAGE_ID, meta.encode()));

    // Allocate all level pages as zero.
    for l in 0..level_count {
        let base = meta.level_base[l as usize];
        let pages = level_page_count(leaves, l);
        for i in 0..pages {
            writes.push((base + i, vec![0u8; PAGE_SIZE]));
        }
    }

    (meta, writes)
}

/// Ensure FSM has at least `desired_leaves` leaf entries.
///
/// Returns updated meta + page writes needed to materialize new level pages and meta.
pub fn ensure_capacity(
    meta: &FsmMeta,
    desired_leaves: u64,
) -> Result<(FsmMeta, Vec<(PageId, Page)>)> {
    if desired_leaves <= meta.leaf_count {
        return Ok((*meta, Vec::new()));
    }

    let new_level_count = levels_for_leaves(desired_leaves).min(MAX_LEVELS as u32);

    let mut new_meta = *meta;
    let mut writes: Vec<(PageId, Page)> = Vec::new();

    // If level_count increases, set base for new levels to next_fsm_page_id.
    let mut next = new_meta.next_fsm_page_id;
    if new_level_count > new_meta.level_count {
        for l in new_meta.level_count..new_level_count {
            new_meta.level_base[l as usize] = next;
            // allocate at least 1 page for this new level
            let pages = level_page_count(desired_leaves, l);
            for i in 0..pages {
                writes.push((next + i, vec![0u8; PAGE_SIZE]));
            }
            next += pages;
        }
        new_meta.level_count = new_level_count;
    }

    // For existing levels, allocate additional pages if needed.
    for l in 0..new_meta.level_count {
        let old_pages = level_page_count(new_meta.leaf_count, l);
        let new_pages = level_page_count(desired_leaves, l);
        if new_pages > old_pages {
            let base = new_meta.level_base[l as usize];
            // We require that this level is stored densely and extendable.
            // If a higher level base overlaps, we'd need relocation; we avoid by placing
            // higher level bases after all lower level pages at initialization.
            for i in old_pages..new_pages {
                writes.push((base + i, vec![0u8; PAGE_SIZE]));
            }
        }
    }

    new_meta.leaf_count = desired_leaves;

    // Update allocation cursor.
    // Conservative: recompute next as max of all allocated regions.
    let mut max_end = new_meta.next_fsm_page_id;
    for l in 0..new_meta.level_count {
        let base = new_meta.level_base[l as usize];
        let pages = level_page_count(desired_leaves, l);
        let end = base + pages;
        if end > max_end {
            max_end = end;
        }
    }
    new_meta.next_fsm_page_id = max_end;

    // Meta page after-image.
    writes.push((FSM_META_PAGE_ID, new_meta.encode()));

    Ok((new_meta, writes))
}

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

pub fn find_candidate(
    meta: &FsmMeta,
    pages: &impl Fn(PageId) -> Option<Page>,
    need: u8,
) -> Result<Option<u64>> {
    let top = meta.level_count.saturating_sub(1);
    let root_max = get_entry(meta, pages, top, 0)?;
    if root_max < need {
        return Ok(None);
    }

    let mut idx = 0u64;
    for level in (0..top).rev() {
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

    if idx >= meta.leaf_count {
        return Ok(None);
    }
    Ok(Some(idx))
}

pub fn update_leaf(
    meta: &FsmMeta,
    pages: &impl Fn(PageId) -> Option<Page>,
    leaf_idx: u64,
    new_class: u8,
) -> Result<Vec<(PageId, Page)>> {
    let mut writes: Vec<(PageId, Page)> = Vec::new();

    let (pid0, p0) = set_entry(meta, pages, 0, leaf_idx, new_class)?;
    writes.push((pid0, p0));

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
