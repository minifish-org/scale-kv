use crate::{Error, PAGE_SIZE, Page, PageId, Result, VALUE_SIZE};

pub const UNDO_PAGE_TYPE: u8 = 1;

pub const UNDO_RECORD_SIZE: usize = 56;

/// Undo pointer stored in data records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UndoPtr {
    pub page_id: PageId,
    pub slot_id: u16,
}

impl UndoPtr {
    pub fn none() -> Option<Self> {
        None
    }
}

/// Undo record.
///
/// Fixed-size record layout (56 bytes):
/// - data_page_id: u64
/// - data_slot_id: u16
/// - pad: u16
/// - prev_undo_page_id: u64
/// - prev_undo_slot_id: u16
/// - pad: u16
/// - old_commit_lsn: u64
/// - old_flags: u16
/// - pad: u16
/// - old_value: [u8; VALUE_SIZE]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UndoRecord {
    pub data_page_id: PageId,
    pub data_slot_id: u16,
    pub prev: Option<UndoPtr>,
    pub old_commit_lsn: u64,
    pub old_flags: u16,
    pub old_value: [u8; VALUE_SIZE],
}

const HDR_SIZE: usize = 8; // type u8 + reserved + free_end u16 + count u16 + reserved

fn read_u16(p: &[u8], off: usize) -> u16 {
    u16::from_le_bytes(p[off..off + 2].try_into().unwrap())
}
fn write_u16(p: &mut [u8], off: usize, v: u16) {
    p[off..off + 2].copy_from_slice(&v.to_le_bytes());
}
fn read_u64(p: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(p[off..off + 8].try_into().unwrap())
}
fn write_u64(p: &mut [u8], off: usize, v: u64) {
    p[off..off + 8].copy_from_slice(&v.to_le_bytes());
}

pub fn new_undo_page() -> Page {
    let mut p = vec![0u8; PAGE_SIZE];
    p[0] = UNDO_PAGE_TYPE;
    // free_end
    write_u16(&mut p, 2, PAGE_SIZE as u16);
    // count
    write_u16(&mut p, 4, 0);
    p
}

pub fn undo_count(page: &[u8]) -> u16 {
    if page.len() != PAGE_SIZE {
        return 0;
    }
    read_u16(page, 4)
}

pub fn append_record(page: &mut [u8], rec: &UndoRecord) -> Result<u16> {
    if page.len() != PAGE_SIZE {
        return Err(Error::InvalidPageSize(page.len(), PAGE_SIZE));
    }
    if page[0] != UNDO_PAGE_TYPE {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "not an undo page",
        )));
    }

    let mut free_end = read_u16(page, 2) as usize;
    let mut count = read_u16(page, 4);
    let dir_bytes = HDR_SIZE + (count as usize + 1) * 2;
    if free_end < dir_bytes + UNDO_RECORD_SIZE {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "undo page full",
        )));
    }

    free_end -= UNDO_RECORD_SIZE;
    let off = free_end;

    write_u64(page, off + 0, rec.data_page_id);
    write_u16(page, off + 8, rec.data_slot_id);
    write_u16(page, off + 10, 0);

    let (prev_pid, prev_sid) = rec.prev.map(|p| (p.page_id, p.slot_id)).unwrap_or((0, 0));
    write_u64(page, off + 12, prev_pid);
    write_u16(page, off + 20, prev_sid);
    write_u16(page, off + 22, 0);

    write_u64(page, off + 24, rec.old_commit_lsn);
    write_u16(page, off + 32, rec.old_flags);
    write_u16(page, off + 34, 0);

    page[off + 36..off + 36 + VALUE_SIZE].copy_from_slice(&rec.old_value);

    // directory stores offset
    write_u16(page, HDR_SIZE + count as usize * 2, off as u16);

    count += 1;
    write_u16(page, 2, free_end as u16);
    write_u16(page, 4, count);
    Ok(count - 1)
}

pub fn read_record(page: &[u8], slot_id: u16) -> Result<UndoRecord> {
    if page.len() != PAGE_SIZE {
        return Err(Error::InvalidPageSize(page.len(), PAGE_SIZE));
    }
    if page[0] != UNDO_PAGE_TYPE {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "not an undo page",
        )));
    }
    let count = read_u16(page, 4);
    if slot_id >= count {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "undo slot out of range",
        )));
    }
    let off = read_u16(page, HDR_SIZE + slot_id as usize * 2) as usize;
    if off + UNDO_RECORD_SIZE > PAGE_SIZE {
        return Err(Error::InvalidPageSize(off + UNDO_RECORD_SIZE, PAGE_SIZE));
    }

    let data_page_id = read_u64(page, off + 0);
    let data_slot_id = read_u16(page, off + 8);

    let prev_page_id = read_u64(page, off + 12);
    let prev_slot_id = read_u16(page, off + 20);
    let prev = if prev_page_id == 0 {
        None
    } else {
        Some(UndoPtr {
            page_id: prev_page_id,
            slot_id: prev_slot_id,
        })
    };

    let old_commit_lsn = read_u64(page, off + 24);
    let old_flags = read_u16(page, off + 32);

    let mut old_value = [0u8; VALUE_SIZE];
    old_value.copy_from_slice(&page[off + 36..off + 36 + VALUE_SIZE]);

    Ok(UndoRecord {
        data_page_id,
        data_slot_id,
        prev,
        old_commit_lsn,
        old_flags,
        old_value,
    })
}
