use crate::secondary_index::SecondaryIndexMutation;
use crate::{Error, KEY_SIZE, PAGE_SIZE, Page, PageId, Result};

pub const SECONDARY_POSTING_LOG_BASE_PAGE_ID: PageId = 3_000_000;

const MAGIC: &[u8; 8] = b"SKPLOG\0\0";
const VERSION: u32 = 1;
const OFF_NEXT_PAGE_ID: usize = 12;
const OFF_USED: usize = 20;
const OFF_DATA: usize = 22;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondaryPostingLogState {
    pub head_page_id: PageId,
    pub tail_page_id: PageId,
    pub next_page_id: PageId,
}

impl Default for SecondaryPostingLogState {
    fn default() -> Self {
        Self {
            head_page_id: 0,
            tail_page_id: 0,
            next_page_id: SECONDARY_POSTING_LOG_BASE_PAGE_ID,
        }
    }
}

pub fn new_log_page() -> Page {
    let mut p = vec![0u8; PAGE_SIZE];
    p[0..8].copy_from_slice(MAGIC);
    p[8..12].copy_from_slice(&VERSION.to_le_bytes());
    p[OFF_NEXT_PAGE_ID..OFF_NEXT_PAGE_ID + 8].copy_from_slice(&0u64.to_le_bytes());
    p[OFF_USED..OFF_USED + 2].copy_from_slice(&0u16.to_le_bytes());
    Page::from(p)
}

pub fn read_next_page_id(page: &[u8]) -> Result<PageId> {
    validate_page(page)?;
    Ok(u64::from_le_bytes(
        page[OFF_NEXT_PAGE_ID..OFF_NEXT_PAGE_ID + 8]
            .try_into()
            .unwrap(),
    ))
}

pub fn write_next_page_id(page: &mut [u8], next: PageId) -> Result<()> {
    validate_page(page)?;
    page[OFF_NEXT_PAGE_ID..OFF_NEXT_PAGE_ID + 8].copy_from_slice(&next.to_le_bytes());
    Ok(())
}

pub fn append_record(
    page: &mut [u8],
    commit_lsn: u64,
    mutation: &SecondaryIndexMutation,
) -> Result<bool> {
    validate_page(page)?;
    let used = u16::from_le_bytes(page[OFF_USED..OFF_USED + 2].try_into().unwrap()) as usize;
    let name = mutation.index_name.as_bytes();
    if name.is_empty() || name.len() > u16::MAX as usize {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "invalid index name length",
        )));
    }
    if mutation.secondary_key.len() > u16::MAX as usize {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "invalid secondary key length",
        )));
    }
    let rec_len = 8 + 1 + 2 + name.len() + 2 + mutation.secondary_key.len() + KEY_SIZE;
    let write_off = OFF_DATA + used;
    if write_off + rec_len > PAGE_SIZE {
        return Ok(false);
    }

    let mut off = write_off;
    page[off..off + 8].copy_from_slice(&commit_lsn.to_le_bytes());
    off += 8;
    page[off] = if mutation.present { 1 } else { 0 };
    off += 1;
    page[off..off + 2].copy_from_slice(&(name.len() as u16).to_le_bytes());
    off += 2;
    page[off..off + name.len()].copy_from_slice(name);
    off += name.len();
    page[off..off + 2].copy_from_slice(&(mutation.secondary_key.len() as u16).to_le_bytes());
    off += 2;
    page[off..off + mutation.secondary_key.len()].copy_from_slice(&mutation.secondary_key);
    off += mutation.secondary_key.len();
    page[off..off + KEY_SIZE].copy_from_slice(&mutation.primary_key);

    let new_used = (used + rec_len) as u16;
    page[OFF_USED..OFF_USED + 2].copy_from_slice(&new_used.to_le_bytes());
    Ok(true)
}

pub fn decode_records(page: &[u8]) -> Result<Vec<(u64, SecondaryIndexMutation)>> {
    validate_page(page)?;
    let used = u16::from_le_bytes(page[OFF_USED..OFF_USED + 2].try_into().unwrap()) as usize;
    if OFF_DATA + used > PAGE_SIZE {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "posting log used bytes out of bounds",
        )));
    }
    let mut out = Vec::new();
    let mut off = OFF_DATA;
    let end = OFF_DATA + used;
    while off < end {
        if off + 8 + 1 + 2 + 2 + KEY_SIZE > end {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "posting log truncated record",
            )));
        }
        let commit_lsn = u64::from_le_bytes(page[off..off + 8].try_into().unwrap());
        off += 8;
        let present = page[off] != 0;
        off += 1;
        let name_len = u16::from_le_bytes(page[off..off + 2].try_into().unwrap()) as usize;
        off += 2;
        if off + name_len > end {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "posting log invalid name length",
            )));
        }
        let index_name = std::str::from_utf8(&page[off..off + name_len])
            .map_err(|e| Error::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))?
            .to_string();
        off += name_len;

        let sec_len = u16::from_le_bytes(page[off..off + 2].try_into().unwrap()) as usize;
        off += 2;
        if off + sec_len + KEY_SIZE > end {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "posting log invalid secondary key length",
            )));
        }
        let secondary_key = page[off..off + sec_len].to_vec();
        off += sec_len;
        let mut pk = [0u8; KEY_SIZE];
        pk.copy_from_slice(&page[off..off + KEY_SIZE]);
        off += KEY_SIZE;

        out.push((
            commit_lsn,
            SecondaryIndexMutation {
                index_name,
                secondary_key,
                primary_key: pk,
                present,
            },
        ));
    }
    Ok(out)
}

fn validate_page(page: &[u8]) -> Result<()> {
    if page.len() != PAGE_SIZE {
        return Err(Error::InvalidPageSize(page.len(), PAGE_SIZE));
    }
    if &page[0..8] != MAGIC {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid posting log page magic",
        )));
    }
    let ver = u32::from_le_bytes(page[8..12].try_into().unwrap());
    if ver != VERSION {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unsupported posting log page version: {ver}"),
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_page_append_and_decode() {
        let mut page = new_log_page().to_vec();
        let m = SecondaryIndexMutation {
            index_name: "tag".to_string(),
            secondary_key: b"aa".to_vec(),
            primary_key: [1u8; KEY_SIZE],
            present: true,
        };
        assert!(append_record(&mut page, 42, &m).unwrap());
        let recs = decode_records(&page).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].0, 42);
        assert_eq!(recs[0].1, m);
    }
}
