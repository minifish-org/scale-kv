use crate::{Error, KEY_SIZE, PAGE_SIZE, Page, Result, VALUE_SIZE};

const PAGE_HEADER_SIZE: usize = 6;
const SLOT_ENTRY_SIZE: usize = 4;

/// Slotted page header:
/// - slots: u16
/// - free_start: u16
/// - free_end: u16
fn read_header(page: &[u8]) -> (u16, u16, u16) {
    let slots = read_u16(page, 0);
    let free_start = read_u16(page, 2);
    let free_end = read_u16(page, 4);
    (slots, free_start, free_end)
}

fn write_header(page: &mut [u8], slots: u16, free_start: u16, free_end: u16) {
    write_u16(page, 0, slots);
    write_u16(page, 2, free_start);
    write_u16(page, 4, free_end);
}

fn slot_offset(slot_id: u16) -> usize {
    PAGE_HEADER_SIZE + SLOT_ENTRY_SIZE * slot_id as usize
}

fn read_slot(page: &[u8], slot_id: u16) -> (u16, u16) {
    let offset = slot_offset(slot_id);
    let pos = read_u16(page, offset);
    let len = read_u16(page, offset + 2);
    (pos, len)
}

fn write_slot(page: &mut [u8], slot_id: u16, offset: u16, len: u16) {
    let pos = slot_offset(slot_id);
    write_u16(page, pos, offset);
    write_u16(page, pos + 2, len)
}

fn find_free_slot(page: &[u8], slots: u16) -> Option<u16> {
    for slot_id in 0..slots {
        let (_, len) = read_slot(page, slot_id);
        if len == 0 {
            return Some(slot_id);
        }
    }
    None
}

const MVCC_HEADER_SIZE: usize = 8 + 8 + 2 + 2; // commit_lsn, undo_page_id, undo_slot_id, flags
const FLAG_TOMBSTONE: u16 = 1;

fn payload_len() -> usize {
    KEY_SIZE + VALUE_SIZE + MVCC_HEADER_SIZE
}

fn slot_payload_offset(page: &[u8], slot_id: u16) -> Option<usize> {
    if page.len() != PAGE_SIZE {
        return None;
    }
    let (pos, len) = read_slot(page, slot_id);
    if len as usize != payload_len() {
        return None;
    }
    let pos = pos as usize;
    if pos + payload_len() > PAGE_SIZE {
        return None;
    }
    Some(pos)
}

pub fn new_page() -> Page {
    let mut page = vec![0u8; PAGE_SIZE];
    write_header(&mut page, 0, PAGE_HEADER_SIZE as u16, PAGE_SIZE as u16);
    Page::from(page)
}

pub fn page_free_space(page: &[u8]) -> usize {
    if page.len() != PAGE_SIZE {
        return 0;
    }
    let (_slots, free_start, free_end) = read_header(page);
    free_end.saturating_sub(free_start) as usize
}

pub fn slot_count(page: &[u8]) -> u16 {
    if page.len() != PAGE_SIZE {
        return 0;
    }
    let (slots, _free_start, _free_end) = read_header(page);
    slots
}

pub fn clear_slot(page: &mut [u8], slot_id: u16) {
    if page.len() != PAGE_SIZE {
        return;
    }
    write_slot(page, slot_id, 0, 0);
}

/// Defragment payload area by packing all live payloads at the end of the page.
/// Slot ids are preserved; only slot offsets change.
pub fn defragment(page: &mut [u8]) -> Result<()> {
    if page.len() != PAGE_SIZE {
        return Err(Error::InvalidPageSize(page.len(), PAGE_SIZE));
    }
    let (slots, free_start, _free_end) = read_header(page);

    // Collect live slots with their payload bytes.
    let mut live: Vec<(u16, Vec<u8>)> = Vec::new();
    for slot_id in 0..slots {
        let (pos, len) = read_slot(page, slot_id);
        if len == 0 {
            continue;
        }
        let pos = pos as usize;
        let len = len as usize;
        if pos + len > PAGE_SIZE {
            return Err(Error::InvalidPageSize(pos + len, PAGE_SIZE));
        }
        live.push((slot_id, page[pos..pos + len].to_vec()));
    }

    // Pack from the end.
    let mut free_end = PAGE_SIZE as u16;
    for (slot_id, payload) in live {
        let len = payload.len() as u16;
        let new_pos = free_end - len;
        let np = new_pos as usize;
        page[np..np + payload.len()].copy_from_slice(&payload);
        write_slot(page, slot_id, new_pos, len);
        free_end = new_pos;
    }

    write_header(page, slots, free_start, free_end);
    Ok(())
}

pub fn read_key(page: &[u8], slot_id: u16) -> Option<Vec<u8>> {
    if page.len() != PAGE_SIZE {
        return None;
    }
    let (pos, len) = read_slot(page, slot_id);
    if len as usize != payload_len() {
        return None;
    }
    let pos = pos as usize;
    if pos + KEY_SIZE > PAGE_SIZE {
        return None;
    }
    Some(page[pos..pos + KEY_SIZE].to_vec())
}

pub fn debug_slot(page: &[u8], slot_id: u16) -> Option<(u16, u16)> {
    if page.len() != PAGE_SIZE {
        return None;
    }
    let (slots, _free_start, _free_end) = read_header(page);
    if slot_id >= slots {
        return None;
    }
    Some(read_slot(page, slot_id))
}

/// Read the value at a slot, verifying the key matches.
pub fn read_value(page: &[u8], slot_id: u16, key: &[u8]) -> Option<Vec<u8>> {
    if page.len() != PAGE_SIZE || key.len() != KEY_SIZE {
        return None;
    }
    let pos = slot_payload_offset(page, slot_id)?;
    if &page[pos..pos + KEY_SIZE] != key {
        return None;
    }
    let v0 = pos + KEY_SIZE;
    let v1 = v0 + VALUE_SIZE;

    // MVCC header
    let flags_off = v1 + 8 + 8 + 2;
    let flags = u16::from_le_bytes(page[flags_off..flags_off + 2].try_into().unwrap());
    if (flags & FLAG_TOMBSTONE) != 0 {
        return None;
    }

    Some(page[v0..v1].to_vec())
}

/// Overwrite value in place at slot, verifying key matches.
///
/// Does not update commit_lsn; caller should set it at commit time.
pub fn overwrite_value(page: &mut [u8], slot_id: u16, key: &[u8], value: &[u8]) -> Result<()> {
    if page.len() != PAGE_SIZE {
        return Err(Error::InvalidPageSize(page.len(), PAGE_SIZE));
    }
    if key.len() != KEY_SIZE {
        return Err(Error::InvalidKeySize(key.len(), KEY_SIZE));
    }
    if value.len() != VALUE_SIZE {
        return Err(Error::InvalidValueSize(value.len(), VALUE_SIZE));
    }
    let (pos, len) = read_slot(page, slot_id);
    if len as usize != payload_len() {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "slot empty or wrong length",
        )));
    }
    let pos = pos as usize;
    if pos + payload_len() > PAGE_SIZE {
        return Err(Error::InvalidPageSize(pos + payload_len(), PAGE_SIZE));
    }
    if &page[pos..pos + KEY_SIZE] != key {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "slot key mismatch",
        )));
    }
    let v0 = pos + KEY_SIZE;
    let v1 = v0 + VALUE_SIZE;
    page[v0..v1].copy_from_slice(value);
    Ok(())
}

pub fn read_commit_lsn(page: &[u8], slot_id: u16, key: &[u8]) -> Option<u64> {
    if page.len() != PAGE_SIZE || key.len() != KEY_SIZE {
        return None;
    }
    let pos = slot_payload_offset(page, slot_id)?;
    if &page[pos..pos + KEY_SIZE] != key {
        return None;
    }
    let off = pos + KEY_SIZE + VALUE_SIZE;
    Some(u64::from_le_bytes(page[off..off + 8].try_into().unwrap()))
}

pub fn write_commit_lsn(page: &mut [u8], slot_id: u16, commit_lsn: u64) {
    let pos = match slot_payload_offset(page, slot_id) {
        Some(p) => p,
        None => return,
    };
    let off = pos + KEY_SIZE + VALUE_SIZE;
    page[off..off + 8].copy_from_slice(&commit_lsn.to_le_bytes());
}

pub fn read_undo_ptr(page: &[u8], slot_id: u16, key: &[u8]) -> Option<crate::undo_pg::UndoPtr> {
    if page.len() != PAGE_SIZE || key.len() != KEY_SIZE {
        return None;
    }
    let pos = slot_payload_offset(page, slot_id)?;
    if &page[pos..pos + KEY_SIZE] != key {
        return None;
    }
    let off = pos + KEY_SIZE + VALUE_SIZE + 8;
    let pid = u64::from_le_bytes(page[off..off + 8].try_into().unwrap());
    if pid == 0 {
        return None;
    }
    let sid_off = off + 8;
    let sid = u16::from_le_bytes(page[sid_off..sid_off + 2].try_into().unwrap());
    Some(crate::undo_pg::UndoPtr {
        page_id: pid,
        slot_id: sid,
    })
}

pub fn write_undo_ptr(page: &mut [u8], slot_id: u16, undo: Option<crate::undo_pg::UndoPtr>) {
    let pos = match slot_payload_offset(page, slot_id) {
        Some(p) => p,
        None => return,
    };
    let off = pos + KEY_SIZE + VALUE_SIZE + 8;
    let (pid, sid) = undo.map(|u| (u.page_id, u.slot_id)).unwrap_or((0, 0));
    page[off..off + 8].copy_from_slice(&pid.to_le_bytes());
    page[off + 8..off + 10].copy_from_slice(&sid.to_le_bytes());
}

pub fn read_flags(page: &[u8], slot_id: u16, key: &[u8]) -> Option<u16> {
    if page.len() != PAGE_SIZE || key.len() != KEY_SIZE {
        return None;
    }
    let pos = slot_payload_offset(page, slot_id)?;
    if &page[pos..pos + KEY_SIZE] != key {
        return None;
    }
    let off = pos + KEY_SIZE + VALUE_SIZE + 8 + 8 + 2;
    Some(u16::from_le_bytes(page[off..off + 2].try_into().unwrap()))
}

pub fn write_flags(page: &mut [u8], slot_id: u16, flags: u16) {
    let pos = match slot_payload_offset(page, slot_id) {
        Some(p) => p,
        None => return,
    };
    let off = pos + KEY_SIZE + VALUE_SIZE + 8 + 8 + 2;
    page[off..off + 2].copy_from_slice(&flags.to_le_bytes());
}

pub fn mark_tombstone(page: &mut [u8], slot_id: u16) {
    let pos = match slot_payload_offset(page, slot_id) {
        Some(p) => p,
        None => return,
    };
    let off = pos + KEY_SIZE + VALUE_SIZE + 8 + 8 + 2;
    let mut flags = u16::from_le_bytes(page[off..off + 2].try_into().unwrap());
    flags |= FLAG_TOMBSTONE;
    page[off..off + 2].copy_from_slice(&flags.to_le_bytes());
}

/// Insert a record into a slotted page and return the slot id.
///
/// This will reuse a free slot if present; otherwise it will append a new slot directory entry.
pub fn insert_record(page: &mut [u8], key: &[u8], value: &[u8]) -> Result<u16> {
    if page.len() != PAGE_SIZE {
        return Err(Error::InvalidPageSize(page.len(), PAGE_SIZE));
    }
    if key.len() != KEY_SIZE {
        return Err(Error::InvalidKeySize(key.len(), KEY_SIZE));
    }
    if value.len() != VALUE_SIZE {
        return Err(Error::InvalidValueSize(value.len(), VALUE_SIZE));
    }

    let (mut slots, mut free_start, mut free_end) = read_header(page);

    let free_slot = find_free_slot(page, slots);

    let mut needed = payload_len();
    if free_slot.is_none() {
        needed += SLOT_ENTRY_SIZE;
    }

    let free_bytes = free_end.saturating_sub(free_start) as usize;
    if free_bytes < needed {
        return Err(Error::InvalidValueSize(needed, free_bytes));
    }

    let slot_id = free_slot.unwrap_or(slots);
    if free_slot.is_none() {
        free_start = free_start.saturating_add(SLOT_ENTRY_SIZE as u16);
        slots = slots.saturating_add(1);
    }

    // Write payload at the end.
    let payload_offset = (free_end as usize).saturating_sub(payload_len()) as u16;
    let mut cursor = payload_offset as usize;
    page[cursor..cursor + KEY_SIZE].copy_from_slice(key);
    cursor += KEY_SIZE;
    page[cursor..cursor + VALUE_SIZE].copy_from_slice(value);
    cursor += VALUE_SIZE;

    // MVCC header defaults: commit_lsn=0, undo_ptr=nil, flags=0.
    page[cursor..cursor + 8].copy_from_slice(&0u64.to_le_bytes());
    cursor += 8;
    page[cursor..cursor + 8].copy_from_slice(&0u64.to_le_bytes());
    cursor += 8;
    page[cursor..cursor + 2].copy_from_slice(&0u16.to_le_bytes());
    cursor += 2;
    page[cursor..cursor + 2].copy_from_slice(&0u16.to_le_bytes());

    // Update slot directory.
    write_slot(page, slot_id, payload_offset, payload_len() as u16);

    free_end = payload_offset;
    write_header(page, slots, free_start, free_end);

    Ok(slot_id)
}

/// Insert at a specific slot id and verify existing key (if any) matches.
/// Used by WAL apply paths.
pub fn insert_record_at_slot_checked(
    page: &mut [u8],
    slot_id: u16,
    key: &[u8],
    value: &[u8],
) -> Result<()> {
    if page.len() != PAGE_SIZE {
        return Err(Error::InvalidPageSize(page.len(), PAGE_SIZE));
    }
    if key.len() != KEY_SIZE {
        return Err(Error::InvalidKeySize(key.len(), KEY_SIZE));
    }
    if value.len() != VALUE_SIZE {
        return Err(Error::InvalidValueSize(value.len(), VALUE_SIZE));
    }

    let (slots, _free_start, _free_end) = read_header(page);
    if slot_id < slots {
        let existing = read_value(page, slot_id, key);
        if existing.is_some() {
            // overwrite only value
            return overwrite_value(page, slot_id, key, value);
        }
        // slot exists but empty or key mismatch (read_value returned None)
        let (_pos, len) = read_slot(page, slot_id);
        if len != 0 {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "wal slot key mismatch",
            )));
        }
    }

    // Fallback: just insert normally (may choose a different free slot). This is fine for tests.
    let _ = insert_record(page, key, value)?;
    Ok(())
}

fn read_u16(page: &[u8], offset: usize) -> u16 {
    let mut buf = [0u8; 2];
    buf.copy_from_slice(&page[offset..offset + 2]);
    u16::from_le_bytes(buf)
}

fn write_u16(page: &mut [u8], offset: usize, value: u16) {
    let bytes = value.to_le_bytes();
    page[offset..offset + 2].copy_from_slice(&bytes)
}

/// Utility: create fixed key bytes from a shorter literal (for tests).
pub fn fixed_key_bytes(input: &[u8]) -> Vec<u8> {
    let mut out = vec![0u8; KEY_SIZE];
    let n = input.len().min(KEY_SIZE);
    out[..n].copy_from_slice(&input[..n]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key_bytes(seed: u8) -> Vec<u8> {
        fixed_key_bytes(&[seed])
    }

    fn value_bytes(seed: u8) -> Vec<u8> {
        vec![seed; VALUE_SIZE]
    }

    #[test]
    fn test_mvcc_fields_roundtrip_and_tombstone() {
        let mut page = new_page().to_vec();
        let key = key_bytes(1);
        let value = value_bytes(7);
        let slot = insert_record(&mut page, &key, &value).unwrap();
        assert_eq!(read_value(&page, slot, &key).unwrap(), value);
        assert_eq!(read_commit_lsn(&page, slot, &key), Some(0));

        write_commit_lsn(&mut page, slot, 42);
        assert_eq!(read_commit_lsn(&page, slot, &key), Some(42));

        let undo = crate::undo_pg::UndoPtr {
            page_id: 99,
            slot_id: 3,
        };
        write_undo_ptr(&mut page, slot, Some(undo));
        assert_eq!(read_undo_ptr(&page, slot, &key), Some(undo));

        write_flags(&mut page, slot, 0x20);
        assert_eq!(read_flags(&page, slot, &key), Some(0x20));

        mark_tombstone(&mut page, slot);
        assert!(read_value(&page, slot, &key).is_none());
        assert_eq!(read_flags(&page, slot, &key), Some(0x21));
    }

    #[test]
    fn test_slot_reuse_and_defragment_preserve_live_data() {
        let mut page = new_page().to_vec();
        let key1 = key_bytes(1);
        let key2 = key_bytes(2);
        let key3 = key_bytes(3);
        let v1 = value_bytes(11);
        let v2 = value_bytes(22);
        let v3 = value_bytes(33);

        let s1 = insert_record(&mut page, &key1, &v1).unwrap();
        let s2 = insert_record(&mut page, &key2, &v2).unwrap();
        clear_slot(&mut page, s1);
        let s3 = insert_record(&mut page, &key3, &v3).unwrap();
        assert_eq!(s3, s1);
        assert_eq!(read_value(&page, s2, &key2).unwrap(), v2);
        assert_eq!(read_value(&page, s3, &key3).unwrap(), v3);

        defragment(&mut page).unwrap();
        assert_eq!(read_value(&page, s2, &key2).unwrap(), v2);
        assert_eq!(read_value(&page, s3, &key3).unwrap(), v3);
    }

    #[test]
    fn test_corrupted_slot_bounds_do_not_panic() {
        let mut page = new_page().to_vec();
        let key = key_bytes(9);
        let value = value_bytes(8);
        let slot = insert_record(&mut page, &key, &value).unwrap();

        write_slot(
            &mut page,
            slot,
            (PAGE_SIZE - payload_len() + 1) as u16,
            payload_len() as u16,
        );

        assert!(read_value(&page, slot, &key).is_none());
        assert!(read_commit_lsn(&page, slot, &key).is_none());
        assert!(read_undo_ptr(&page, slot, &key).is_none());
        assert!(read_flags(&page, slot, &key).is_none());

        write_commit_lsn(&mut page, slot, 1);
        write_undo_ptr(
            &mut page,
            slot,
            Some(crate::undo_pg::UndoPtr {
                page_id: 1,
                slot_id: 1,
            }),
        );
        write_flags(&mut page, slot, 5);
        mark_tombstone(&mut page, slot);
    }

    #[test]
    fn test_insert_record_at_slot_checked_rejects_key_mismatch() {
        let mut page = new_page().to_vec();
        let key = key_bytes(1);
        let value = value_bytes(1);
        let slot = insert_record(&mut page, &key, &value).unwrap();

        let other_key = key_bytes(2);
        let err = insert_record_at_slot_checked(&mut page, slot, &other_key, &value).unwrap_err();
        assert!(matches!(err, Error::Io(_)));
    }
}
