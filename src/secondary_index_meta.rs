use crate::secondary_index::{SecondaryIndexDefinition, SecondaryIndexKind};
use crate::secondary_posting_log::{SECONDARY_POSTING_LOG_BASE_PAGE_ID, SecondaryPostingLogState};
use crate::{Error, PAGE_SIZE, Page, PageId, Result, VALUE_SIZE};

pub const SECONDARY_INDEX_META_PAGE_ID: PageId = 9;

const MAGIC: &[u8; 8] = b"SKS2IDX\0";
const VERSION_V1: u32 = 1;
const VERSION_V2: u32 = 2;
const NAME_MAX: usize = 48;
const ENTRY_SIZE: usize = 2 + NAME_MAX + 4 + 4 + 1;
const KIND_BTREE: u8 = 1;

const OFF_COUNT_V1: usize = 12;
const OFF_ENTRIES_V1: usize = 14;

const OFF_COUNT_V2: usize = 12;
const OFF_LOG_HEAD_V2: usize = 14;
const OFF_LOG_TAIL_V2: usize = 22;
const OFF_LOG_NEXT_V2: usize = 30;
const OFF_ENTRIES_V2: usize = 38;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondaryIndexCatalog {
    pub defs: Vec<SecondaryIndexDefinition>,
    pub posting_log: SecondaryPostingLogState,
}

pub fn encode_secondary_index_catalog(catalog: &SecondaryIndexCatalog) -> Result<Page> {
    let max_entries = (PAGE_SIZE - OFF_ENTRIES_V2) / ENTRY_SIZE;
    if catalog.defs.len() > max_entries {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "too many secondary indexes: {} (max {})",
                catalog.defs.len(),
                max_entries
            ),
        )));
    }

    let mut p = vec![0u8; PAGE_SIZE];
    p[0..8].copy_from_slice(MAGIC);
    p[8..12].copy_from_slice(&VERSION_V2.to_le_bytes());
    p[OFF_COUNT_V2..OFF_COUNT_V2 + 2].copy_from_slice(&(catalog.defs.len() as u16).to_le_bytes());
    p[OFF_LOG_HEAD_V2..OFF_LOG_HEAD_V2 + 8]
        .copy_from_slice(&catalog.posting_log.head_page_id.to_le_bytes());
    p[OFF_LOG_TAIL_V2..OFF_LOG_TAIL_V2 + 8]
        .copy_from_slice(&catalog.posting_log.tail_page_id.to_le_bytes());
    p[OFF_LOG_NEXT_V2..OFF_LOG_NEXT_V2 + 8]
        .copy_from_slice(&catalog.posting_log.next_page_id.to_le_bytes());

    let mut off = OFF_ENTRIES_V2;
    for def in &catalog.defs {
        let name_bytes = def.name.as_bytes();
        if name_bytes.is_empty() || name_bytes.len() > NAME_MAX {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "secondary index name len must be 1..{}: {}",
                    NAME_MAX,
                    def.name
                ),
            )));
        }
        p[off..off + 2].copy_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        off += 2;
        p[off..off + NAME_MAX].fill(0);
        p[off..off + name_bytes.len()].copy_from_slice(name_bytes);
        off += NAME_MAX;

        match def.kind {
            SecondaryIndexKind::Btree {
                value_offset,
                value_len,
            } => {
                if value_len == 0 || value_offset + value_len > VALUE_SIZE {
                    return Err(Error::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "secondary index range out of bounds: offset={} len={} value_size={}",
                            value_offset, value_len, VALUE_SIZE
                        ),
                    )));
                }
                p[off..off + 4].copy_from_slice(&(value_offset as u32).to_le_bytes());
                off += 4;
                p[off..off + 4].copy_from_slice(&(value_len as u32).to_le_bytes());
                off += 4;
                p[off] = KIND_BTREE;
                off += 1;
            }
        }
    }

    Ok(Page::from(p))
}

pub fn decode_secondary_index_catalog(page: &[u8]) -> Result<SecondaryIndexCatalog> {
    if page.len() != PAGE_SIZE {
        return Err(Error::InvalidPageSize(page.len(), PAGE_SIZE));
    }
    if &page[0..8] != MAGIC {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid secondary index meta magic",
        )));
    }
    let ver = u32::from_le_bytes(page[8..12].try_into().unwrap());
    match ver {
        VERSION_V1 => decode_v1(page),
        VERSION_V2 => decode_v2(page),
        _ => Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unsupported secondary index meta version: {ver}"),
        ))),
    }
}

fn decode_v1(page: &[u8]) -> Result<SecondaryIndexCatalog> {
    let defs = decode_defs(page, OFF_COUNT_V1, OFF_ENTRIES_V1)?;
    Ok(SecondaryIndexCatalog {
        defs,
        posting_log: SecondaryPostingLogState {
            head_page_id: 0,
            tail_page_id: 0,
            next_page_id: SECONDARY_POSTING_LOG_BASE_PAGE_ID,
        },
    })
}

fn decode_v2(page: &[u8]) -> Result<SecondaryIndexCatalog> {
    let defs = decode_defs(page, OFF_COUNT_V2, OFF_ENTRIES_V2)?;
    let head_page_id = u64::from_le_bytes(page[OFF_LOG_HEAD_V2..OFF_LOG_HEAD_V2 + 8].try_into().unwrap());
    let tail_page_id = u64::from_le_bytes(page[OFF_LOG_TAIL_V2..OFF_LOG_TAIL_V2 + 8].try_into().unwrap());
    let mut next_page_id = u64::from_le_bytes(page[OFF_LOG_NEXT_V2..OFF_LOG_NEXT_V2 + 8].try_into().unwrap());
    if next_page_id < SECONDARY_POSTING_LOG_BASE_PAGE_ID {
        next_page_id = SECONDARY_POSTING_LOG_BASE_PAGE_ID;
    }
    Ok(SecondaryIndexCatalog {
        defs,
        posting_log: SecondaryPostingLogState {
            head_page_id,
            tail_page_id,
            next_page_id,
        },
    })
}

fn decode_defs(
    page: &[u8],
    off_count: usize,
    off_entries: usize,
) -> Result<Vec<SecondaryIndexDefinition>> {
    let n = u16::from_le_bytes(page[off_count..off_count + 2].try_into().unwrap()) as usize;
    let max_entries = (PAGE_SIZE - off_entries) / ENTRY_SIZE;
    if n > max_entries {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("invalid secondary index count: {n}"),
        )));
    }

    let mut out = Vec::with_capacity(n);
    let mut off = off_entries;
    for _ in 0..n {
        let name_len = u16::from_le_bytes(page[off..off + 2].try_into().unwrap()) as usize;
        off += 2;
        if name_len == 0 || name_len > NAME_MAX {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid secondary index name length: {name_len}"),
            )));
        }
        let name = std::str::from_utf8(&page[off..off + name_len])
            .map_err(|e| Error::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))?
            .to_string();
        off += NAME_MAX;

        let value_offset = u32::from_le_bytes(page[off..off + 4].try_into().unwrap()) as usize;
        off += 4;
        let value_len = u32::from_le_bytes(page[off..off + 4].try_into().unwrap()) as usize;
        off += 4;
        let kind = page[off];
        off += 1;

        let kind = match kind {
            KIND_BTREE => SecondaryIndexKind::Btree {
                value_offset,
                value_len,
            },
            _ => {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unsupported secondary index kind: {kind}"),
                )));
            }
        };
        out.push(SecondaryIndexDefinition { name, kind });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_catalog_roundtrip() {
        let catalog = SecondaryIndexCatalog {
            defs: vec![
                SecondaryIndexDefinition {
                    name: "tag".to_string(),
                    kind: SecondaryIndexKind::Btree {
                        value_offset: 0,
                        value_len: 2,
                    },
                },
                SecondaryIndexDefinition {
                    name: "region".to_string(),
                    kind: SecondaryIndexKind::Btree {
                        value_offset: 8,
                        value_len: 4,
                    },
                },
            ],
            posting_log: SecondaryPostingLogState {
                head_page_id: 3_000_000,
                tail_page_id: 3_000_100,
                next_page_id: 3_000_101,
            },
        };
        let page = encode_secondary_index_catalog(&catalog).unwrap();
        let got = decode_secondary_index_catalog(&page).unwrap();
        assert_eq!(got, catalog);
    }
}
