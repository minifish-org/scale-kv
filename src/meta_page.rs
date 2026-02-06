use crate::{Error, PAGE_SIZE, Page, PageId, Result};

pub const META_PAGE_ID: PageId = 0;

const MAGIC: &[u8; 8] = b"SKVMETA\0";
const VERSION: u32 = 1;

/// Meta page layout (fixed):
/// - [0..8)   magic
/// - [8..12)  version (u32 LE)
/// - [12..20) root_page_id (u64 LE)
/// - [20..28) next_bptree_page_id (u64 LE)
/// - [28..36) next_data_page_id (u64 LE)
/// - [36..44) next_undo_page_id (u64 LE)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetaPage {
    pub root_page_id: PageId,
    pub next_bptree_page_id: PageId,
    pub next_data_page_id: PageId,
    pub next_undo_page_id: PageId,
}

impl MetaPage {
    pub fn encode(&self) -> Page {
        let mut page = vec![0u8; PAGE_SIZE];
        page[0..8].copy_from_slice(MAGIC);
        page[8..12].copy_from_slice(&VERSION.to_le_bytes());
        page[12..20].copy_from_slice(&self.root_page_id.to_le_bytes());
        page[20..28].copy_from_slice(&self.next_bptree_page_id.to_le_bytes());
        page[28..36].copy_from_slice(&self.next_data_page_id.to_le_bytes());
        page[36..44].copy_from_slice(&self.next_undo_page_id.to_le_bytes());
        page
    }

    pub fn decode(page: &[u8]) -> Result<Self> {
        if page.len() != PAGE_SIZE {
            return Err(Error::InvalidPageSize(page.len(), PAGE_SIZE));
        }
        if &page[0..8] != MAGIC {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid meta page magic",
            )));
        }
        let mut ver = [0u8; 4];
        ver.copy_from_slice(&page[8..12]);
        let ver = u32::from_le_bytes(ver);
        if ver != VERSION {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unsupported meta version: {ver}"),
            )));
        }
        let mut root = [0u8; 8];
        root.copy_from_slice(&page[12..20]);
        let mut next_bptree = [0u8; 8];
        next_bptree.copy_from_slice(&page[20..28]);
        let mut next_data = [0u8; 8];
        next_data.copy_from_slice(&page[28..36]);
        let mut next_undo = [0u8; 8];
        next_undo.copy_from_slice(&page[36..44]);
        Ok(Self {
            root_page_id: u64::from_le_bytes(root),
            next_bptree_page_id: u64::from_le_bytes(next_bptree),
            next_data_page_id: u64::from_le_bytes(next_data),
            next_undo_page_id: u64::from_le_bytes(next_undo),
        })
    }
}
