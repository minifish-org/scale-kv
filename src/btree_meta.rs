use crate::{Error, PAGE_SIZE, Page, PageId, Result};

pub const BTREE_META_PAGE_ID: PageId = 8;

const MAGIC: &[u8; 8] = b"SKBTREE\0";
const VERSION: u32 = 1;

/// B+Tree meta page.
///
/// Layout:
/// - [0..8) magic
/// - [8..12) version
/// - [12..20) root_page_id
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BtreeMeta {
    pub root_page_id: PageId,
}

impl BtreeMeta {
    pub fn encode(&self) -> Page {
        let mut p = vec![0u8; PAGE_SIZE];
        p[0..8].copy_from_slice(MAGIC);
        p[8..12].copy_from_slice(&VERSION.to_le_bytes());
        p[12..20].copy_from_slice(&self.root_page_id.to_le_bytes());
        Page::from(p)
    }

    pub fn decode(page: &[u8]) -> Result<Self> {
        if page.len() != PAGE_SIZE {
            return Err(Error::InvalidPageSize(page.len(), PAGE_SIZE));
        }
        if &page[0..8] != MAGIC {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid btree meta magic",
            )));
        }
        let ver = u32::from_le_bytes(page[8..12].try_into().unwrap());
        if ver != VERSION {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unsupported btree meta version: {ver}"),
            )));
        }
        let root = u64::from_le_bytes(page[12..20].try_into().unwrap());
        Ok(Self { root_page_id: root })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::ErrorKind;

    #[test]
    fn test_encode_decode_roundtrip() {
        let meta = BtreeMeta { root_page_id: 123 };
        let page = meta.encode();
        let decoded = BtreeMeta::decode(&page).unwrap();
        assert_eq!(decoded, meta);
    }

    #[test]
    fn test_decode_rejects_invalid_size_magic_and_version() {
        let err = BtreeMeta::decode(&[0u8; 8]).unwrap_err();
        assert!(matches!(err, Error::InvalidPageSize(_, PAGE_SIZE)));

        let mut page = vec![0u8; PAGE_SIZE];
        page[0..8].copy_from_slice(b"BADMETA!");
        let err = BtreeMeta::decode(&page).unwrap_err();
        assert!(matches!(err, Error::Io(ref ioe) if ioe.kind() == ErrorKind::InvalidData));

        let mut page = BtreeMeta { root_page_id: 1 }.encode().to_vec();
        page[8..12].copy_from_slice(&999u32.to_le_bytes());
        let err = BtreeMeta::decode(&page).unwrap_err();
        assert!(matches!(err, Error::Io(ref ioe) if ioe.kind() == ErrorKind::InvalidData));
    }
}
