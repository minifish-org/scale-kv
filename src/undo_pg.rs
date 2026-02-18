use crate::{Error, PAGE_SIZE, Page, PageId, Result, VALUE_SIZE};
use bytes::Bytes;

pub const UNDO_PAGE_TYPE: u8 = 1;
pub const UNDO_SEGMENT_PAGE_TYPE: u8 = 2;

pub const SEGMENT_STATE_IN_PROGRESS: u8 = 0;
pub const SEGMENT_STATE_COMMITTED: u8 = 1;
pub const SEGMENT_STATE_ABORTED: u8 = 2;
pub const SEGMENT_STATE_PURGED: u8 = 3;

pub const UNDO_RECORD_SIZE: usize = 56 + VALUE_SIZE;

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

/// Undo record with both version-chain and txn-chain links.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UndoRecord {
    pub data_page_id: PageId,
    pub data_slot_id: u16,
    pub prev: Option<UndoPtr>,
    pub txn_id: u64,
    pub txn_next: Option<UndoPtr>,
    pub old_commit_lsn: u64,
    pub old_flags: u16,
    pub old_value: [u8; VALUE_SIZE],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UndoRecordRef {
    pub data_page_id: PageId,
    pub data_slot_id: u16,
    pub prev: Option<UndoPtr>,
    pub txn_id: u64,
    pub txn_next: Option<UndoPtr>,
    pub old_commit_lsn: u64,
    pub old_flags: u16,
    pub old_value: Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UndoSegmentHeader {
    pub txn_id: u64,
    pub begin_lsn: u64,
    pub commit_lsn: u64,
    pub state: u8,
    pub first_page_id: PageId,
    pub last_page_id: PageId,
    pub record_count: u32,
    pub history_prev: PageId,
    pub history_next: PageId,
}

const OFF_PAGE_TYPE: usize = 0;
const OFF_FREE_END: usize = 2;
const OFF_COUNT: usize = 4;
const OFF_PAGE_NEXT: usize = 8;

const BASE_HDR_SIZE: usize = 16;

const OFF_SEG_TXN_ID: usize = 16;
const OFF_SEG_BEGIN_LSN: usize = 24;
const OFF_SEG_COMMIT_LSN: usize = 32;
const OFF_SEG_STATE: usize = 40;
const OFF_SEG_FIRST_PAGE_ID: usize = 48;
const OFF_SEG_LAST_PAGE_ID: usize = 56;
const OFF_SEG_RECORD_COUNT: usize = 64;
const OFF_SEG_HISTORY_PREV: usize = 72;
const OFF_SEG_HISTORY_NEXT: usize = 80;
const SEG_HDR_SIZE: usize = 72;
const SEG_PAGE_HDR_SIZE: usize = BASE_HDR_SIZE + SEG_HDR_SIZE;

fn read_u16(p: &[u8], off: usize) -> u16 {
    u16::from_le_bytes(p[off..off + 2].try_into().unwrap())
}
fn write_u16(p: &mut [u8], off: usize, v: u16) {
    p[off..off + 2].copy_from_slice(&v.to_le_bytes());
}
fn read_u32(p: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(p[off..off + 4].try_into().unwrap())
}
fn write_u32(p: &mut [u8], off: usize, v: u32) {
    p[off..off + 4].copy_from_slice(&v.to_le_bytes());
}
fn read_u64(p: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(p[off..off + 8].try_into().unwrap())
}
fn write_u64(p: &mut [u8], off: usize, v: u64) {
    p[off..off + 8].copy_from_slice(&v.to_le_bytes());
}

fn page_hdr_size(page: &[u8]) -> Result<usize> {
    if page.len() != PAGE_SIZE {
        return Err(Error::InvalidPageSize(page.len(), PAGE_SIZE));
    }
    match page[OFF_PAGE_TYPE] {
        UNDO_PAGE_TYPE => Ok(BASE_HDR_SIZE),
        UNDO_SEGMENT_PAGE_TYPE => Ok(SEG_PAGE_HDR_SIZE),
        _ => Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "not an undo page",
        ))),
    }
}

pub fn new_undo_page() -> Page {
    let mut p = vec![0u8; PAGE_SIZE];
    p[OFF_PAGE_TYPE] = UNDO_PAGE_TYPE;
    write_u16(&mut p, OFF_FREE_END, PAGE_SIZE as u16);
    write_u16(&mut p, OFF_COUNT, 0);
    write_u64(&mut p, OFF_PAGE_NEXT, 0);
    Page::from(p)
}

pub fn new_undo_segment_page(txn_id: u64, page_id: PageId) -> Page {
    let mut p = vec![0u8; PAGE_SIZE];
    p[OFF_PAGE_TYPE] = UNDO_SEGMENT_PAGE_TYPE;
    write_u16(&mut p, OFF_FREE_END, PAGE_SIZE as u16);
    write_u16(&mut p, OFF_COUNT, 0);
    write_u64(&mut p, OFF_PAGE_NEXT, 0);
    write_u64(&mut p, OFF_SEG_TXN_ID, txn_id);
    write_u64(&mut p, OFF_SEG_BEGIN_LSN, 0);
    write_u64(&mut p, OFF_SEG_COMMIT_LSN, 0);
    p[OFF_SEG_STATE] = SEGMENT_STATE_IN_PROGRESS;
    write_u64(&mut p, OFF_SEG_FIRST_PAGE_ID, page_id);
    write_u64(&mut p, OFF_SEG_LAST_PAGE_ID, page_id);
    write_u32(&mut p, OFF_SEG_RECORD_COUNT, 0);
    write_u64(&mut p, OFF_SEG_HISTORY_PREV, 0);
    write_u64(&mut p, OFF_SEG_HISTORY_NEXT, 0);
    Page::from(p)
}

pub fn undo_count(page: &[u8]) -> u16 {
    if page.len() != PAGE_SIZE {
        return 0;
    }
    read_u16(page, OFF_COUNT)
}

pub fn page_next_id(page: &[u8]) -> Result<PageId> {
    if page.len() != PAGE_SIZE {
        return Err(Error::InvalidPageSize(page.len(), PAGE_SIZE));
    }
    if page[OFF_PAGE_TYPE] != UNDO_PAGE_TYPE && page[OFF_PAGE_TYPE] != UNDO_SEGMENT_PAGE_TYPE {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "not an undo page",
        )));
    }
    Ok(read_u64(page, OFF_PAGE_NEXT))
}

pub fn set_page_next_id(page: &mut [u8], next: PageId) -> Result<()> {
    let _ = page_hdr_size(page)?;
    write_u64(page, OFF_PAGE_NEXT, next);
    Ok(())
}

pub fn read_segment_header(page: &[u8]) -> Result<UndoSegmentHeader> {
    if page.len() != PAGE_SIZE {
        return Err(Error::InvalidPageSize(page.len(), PAGE_SIZE));
    }
    if page[OFF_PAGE_TYPE] != UNDO_SEGMENT_PAGE_TYPE {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "not a segment-head undo page",
        )));
    }
    Ok(UndoSegmentHeader {
        txn_id: read_u64(page, OFF_SEG_TXN_ID),
        begin_lsn: read_u64(page, OFF_SEG_BEGIN_LSN),
        commit_lsn: read_u64(page, OFF_SEG_COMMIT_LSN),
        state: page[OFF_SEG_STATE],
        first_page_id: read_u64(page, OFF_SEG_FIRST_PAGE_ID),
        last_page_id: read_u64(page, OFF_SEG_LAST_PAGE_ID),
        record_count: read_u32(page, OFF_SEG_RECORD_COUNT),
        history_prev: read_u64(page, OFF_SEG_HISTORY_PREV),
        history_next: read_u64(page, OFF_SEG_HISTORY_NEXT),
    })
}

pub fn write_segment_header(page: &mut [u8], hdr: UndoSegmentHeader) -> Result<()> {
    if page.len() != PAGE_SIZE {
        return Err(Error::InvalidPageSize(page.len(), PAGE_SIZE));
    }
    if page[OFF_PAGE_TYPE] != UNDO_SEGMENT_PAGE_TYPE {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "not a segment-head undo page",
        )));
    }
    write_u64(page, OFF_SEG_TXN_ID, hdr.txn_id);
    write_u64(page, OFF_SEG_BEGIN_LSN, hdr.begin_lsn);
    write_u64(page, OFF_SEG_COMMIT_LSN, hdr.commit_lsn);
    page[OFF_SEG_STATE] = hdr.state;
    write_u64(page, OFF_SEG_FIRST_PAGE_ID, hdr.first_page_id);
    write_u64(page, OFF_SEG_LAST_PAGE_ID, hdr.last_page_id);
    write_u32(page, OFF_SEG_RECORD_COUNT, hdr.record_count);
    write_u64(page, OFF_SEG_HISTORY_PREV, hdr.history_prev);
    write_u64(page, OFF_SEG_HISTORY_NEXT, hdr.history_next);
    Ok(())
}

pub fn append_record(page: &mut [u8], rec: &UndoRecord) -> Result<u16> {
    let hdr_size = page_hdr_size(page)?;
    let mut free_end = read_u16(page, OFF_FREE_END) as usize;
    let mut count = read_u16(page, OFF_COUNT);
    let dir_bytes = hdr_size + (count as usize + 1) * 2;
    if free_end < dir_bytes + UNDO_RECORD_SIZE {
        return Err(Error::Io(std::io::Error::other(
            "undo page full",
        )));
    }

    free_end -= UNDO_RECORD_SIZE;
    let off = free_end;

    write_u64(page, off, rec.data_page_id);
    write_u16(page, off + 8, rec.data_slot_id);
    write_u16(page, off + 10, 0);

    let (prev_pid, prev_sid) = rec.prev.map(|p| (p.page_id, p.slot_id)).unwrap_or((0, 0));
    write_u64(page, off + 12, prev_pid);
    write_u16(page, off + 20, prev_sid);
    write_u16(page, off + 22, 0);

    write_u64(page, off + 24, rec.txn_id);

    let (txn_next_pid, txn_next_sid) = rec
        .txn_next
        .map(|p| (p.page_id, p.slot_id))
        .unwrap_or((0, 0));
    write_u64(page, off + 32, txn_next_pid);
    write_u16(page, off + 40, txn_next_sid);
    write_u16(page, off + 42, 0);

    write_u64(page, off + 44, rec.old_commit_lsn);
    write_u16(page, off + 52, rec.old_flags);
    write_u16(page, off + 54, 0);

    page[off + 56..off + 56 + VALUE_SIZE].copy_from_slice(&rec.old_value);

    write_u16(page, hdr_size + count as usize * 2, off as u16);

    count += 1;
    write_u16(page, OFF_FREE_END, free_end as u16);
    write_u16(page, OFF_COUNT, count);
    Ok(count - 1)
}

pub fn read_record(page: &[u8], slot_id: u16) -> Result<UndoRecord> {
    let hdr_size = page_hdr_size(page)?;
    let count = read_u16(page, OFF_COUNT);
    if slot_id >= count {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "undo slot out of range",
        )));
    }
    let off = read_u16(page, hdr_size + slot_id as usize * 2) as usize;
    if off + UNDO_RECORD_SIZE > PAGE_SIZE {
        return Err(Error::InvalidPageSize(off + UNDO_RECORD_SIZE, PAGE_SIZE));
    }

    let data_page_id = read_u64(page, off);
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

    let txn_id = read_u64(page, off + 24);

    let txn_next_page_id = read_u64(page, off + 32);
    let txn_next_slot_id = read_u16(page, off + 40);
    let txn_next = if txn_next_page_id == 0 {
        None
    } else {
        Some(UndoPtr {
            page_id: txn_next_page_id,
            slot_id: txn_next_slot_id,
        })
    };

    let old_commit_lsn = read_u64(page, off + 44);
    let old_flags = read_u16(page, off + 52);
    let mut old_value = [0u8; VALUE_SIZE];
    old_value.copy_from_slice(&page[off + 56..off + 56 + VALUE_SIZE]);
    Ok(UndoRecord {
        data_page_id,
        data_slot_id,
        prev,
        txn_id,
        txn_next,
        old_commit_lsn,
        old_flags,
        old_value,
    })
}

pub fn read_record_ref(page: &Page, slot_id: u16) -> Result<UndoRecordRef> {
    let hdr_size = page_hdr_size(page)?;
    let count = read_u16(page, OFF_COUNT);
    if slot_id >= count {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "undo slot out of range",
        )));
    }
    let off = read_u16(page, hdr_size + slot_id as usize * 2) as usize;
    if off + UNDO_RECORD_SIZE > PAGE_SIZE {
        return Err(Error::InvalidPageSize(off + UNDO_RECORD_SIZE, PAGE_SIZE));
    }

    let data_page_id = read_u64(page, off);
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

    let txn_id = read_u64(page, off + 24);

    let txn_next_page_id = read_u64(page, off + 32);
    let txn_next_slot_id = read_u16(page, off + 40);
    let txn_next = if txn_next_page_id == 0 {
        None
    } else {
        Some(UndoPtr {
            page_id: txn_next_page_id,
            slot_id: txn_next_slot_id,
        })
    };

    let old_commit_lsn = read_u64(page, off + 44);
    let old_flags = read_u16(page, off + 52);
    let old_value = page.slice(off + 56..off + 56 + VALUE_SIZE);
    Ok(UndoRecordRef {
        data_page_id,
        data_slot_id,
        prev,
        txn_id,
        txn_next,
        old_commit_lsn,
        old_flags,
        old_value,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_record(seed: u8) -> UndoRecord {
        UndoRecord {
            data_page_id: 10 + seed as u64,
            data_slot_id: seed as u16,
            prev: Some(UndoPtr {
                page_id: 20 + seed as u64,
                slot_id: 2,
            }),
            txn_id: 30 + seed as u64,
            txn_next: Some(UndoPtr {
                page_id: 40 + seed as u64,
                slot_id: 3,
            }),
            old_commit_lsn: 50 + seed as u64,
            old_flags: seed as u16,
            old_value: [seed; VALUE_SIZE],
        }
    }

    #[test]
    fn test_segment_header_roundtrip_and_page_next() {
        let mut page = new_undo_segment_page(7, 123).to_vec();
        assert_eq!(page_next_id(&page).unwrap(), 0);
        set_page_next_id(&mut page, 999).unwrap();
        assert_eq!(page_next_id(&page).unwrap(), 999);

        let mut hdr = read_segment_header(&page).unwrap();
        assert_eq!(hdr.txn_id, 7);
        assert_eq!(hdr.first_page_id, 123);
        assert_eq!(hdr.last_page_id, 123);
        assert_eq!(hdr.state, SEGMENT_STATE_IN_PROGRESS);

        hdr.begin_lsn = 11;
        hdr.commit_lsn = 12;
        hdr.record_count = 13;
        hdr.state = SEGMENT_STATE_COMMITTED;
        hdr.history_prev = 1001;
        hdr.history_next = 1002;
        write_segment_header(&mut page, hdr).unwrap();
        assert_eq!(read_segment_header(&page).unwrap(), hdr);
    }

    #[test]
    fn test_append_and_read_roundtrip() {
        let mut page = new_undo_page().to_vec();
        let rec = sample_record(9);
        let slot = append_record(&mut page, &rec).unwrap();
        assert_eq!(slot, 0);
        assert_eq!(undo_count(&page), 1);
        assert_eq!(read_record(&page, slot).unwrap(), rec);

        let page_bytes = Page::copy_from_slice(&page);
        let rec_ref = read_record_ref(&page_bytes, slot).unwrap();
        assert_eq!(rec_ref.data_page_id, rec.data_page_id);
        assert_eq!(rec_ref.data_slot_id, rec.data_slot_id);
        assert_eq!(rec_ref.prev, rec.prev);
        assert_eq!(rec_ref.txn_id, rec.txn_id);
        assert_eq!(rec_ref.txn_next, rec.txn_next);
        assert_eq!(rec_ref.old_commit_lsn, rec.old_commit_lsn);
        assert_eq!(rec_ref.old_flags, rec.old_flags);
        assert_eq!(rec_ref.old_value.as_ref(), rec.old_value.as_slice());
    }

    #[test]
    fn test_append_until_full_returns_error() {
        let mut page = new_undo_page().to_vec();
        let rec = sample_record(1);
        let mut inserted = 0usize;
        loop {
            match append_record(&mut page, &rec) {
                Ok(_) => inserted += 1,
                Err(Error::Io(err)) if err.kind() == std::io::ErrorKind::Other => break,
                Err(other) => panic!("unexpected error: {other:?}"),
            }
        }
        assert_eq!(undo_count(&page) as usize, inserted);
        assert!(inserted > 0);
    }

    #[test]
    fn test_invalid_page_type_and_slot_errors() {
        let mut raw = vec![0u8; PAGE_SIZE];
        raw[OFF_PAGE_TYPE] = 0;
        assert!(page_next_id(&raw).is_err());
        assert!(read_segment_header(&raw).is_err());
        assert!(
            write_segment_header(
                &mut raw,
                UndoSegmentHeader {
                    txn_id: 0,
                    begin_lsn: 0,
                    commit_lsn: 0,
                    state: SEGMENT_STATE_ABORTED,
                    first_page_id: 0,
                    last_page_id: 0,
                    record_count: 0,
                    history_prev: 0,
                    history_next: 0,
                }
            )
            .is_err()
        );

        let page = new_undo_page();
        assert!(read_record(&page, 0).is_err());
        assert!(read_record_ref(&page, 0).is_err());
    }
}
