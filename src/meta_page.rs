use crate::{Error, PAGE_SIZE, Page, PageId, Result};

pub const META_PAGE_ID: PageId = 0;

const MAGIC: &[u8; 8] = b"SKVMETA\0";
const VERSION_V3: u32 = 3;

/// Meta page layout (V3):
/// - [0..8)   magic
/// - [8..12)  version (u32 LE)
/// - [12..20) root_page_id (u64 LE)
/// - [20..28) next_bptree_page_id (u64 LE)
/// - [28..36) next_data_page_id (u64 LE)
/// - [36..44) next_undo_page_id (u64 LE)
/// - [44..48) undo_free_len (u32 LE)
/// - [48..]   undo_free_ids: up to UNDO_FREE_CAP u64 values
/// - [H..H+8) undo_history_head (u64 LE)
/// - [H+8..H+16) undo_history_tail (u64 LE)
pub const UNDO_FREE_CAP: usize = 1024;
const OFF_UNDO_FREE_LEN: usize = 44;
const OFF_UNDO_FREE_IDS: usize = 48;
const OFF_UNDO_HISTORY_HEAD: usize = OFF_UNDO_FREE_IDS + UNDO_FREE_CAP * 8;
const OFF_UNDO_HISTORY_TAIL: usize = OFF_UNDO_HISTORY_HEAD + 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaPage {
    pub root_page_id: PageId,
    pub next_bptree_page_id: PageId,
    pub next_data_page_id: PageId,
    pub next_undo_page_id: PageId,

    pub undo_free: Vec<PageId>,
    pub undo_history_head: PageId,
    pub undo_history_tail: PageId,
}

impl MetaPage {
    pub fn encode(&self) -> Page {
        let mut page = vec![0u8; PAGE_SIZE];
        page[0..8].copy_from_slice(MAGIC);
        page[8..12].copy_from_slice(&VERSION_V3.to_le_bytes());
        page[12..20].copy_from_slice(&self.root_page_id.to_le_bytes());
        page[20..28].copy_from_slice(&self.next_bptree_page_id.to_le_bytes());
        page[28..36].copy_from_slice(&self.next_data_page_id.to_le_bytes());
        page[36..44].copy_from_slice(&self.next_undo_page_id.to_le_bytes());

        let n = self.undo_free.len().min(UNDO_FREE_CAP) as u32;
        page[OFF_UNDO_FREE_LEN..OFF_UNDO_FREE_LEN + 4].copy_from_slice(&n.to_le_bytes());
        let mut off = OFF_UNDO_FREE_IDS;
        for pid in self.undo_free.iter().take(UNDO_FREE_CAP) {
            page[off..off + 8].copy_from_slice(&pid.to_le_bytes());
            off += 8;
        }
        page[OFF_UNDO_HISTORY_HEAD..OFF_UNDO_HISTORY_HEAD + 8]
            .copy_from_slice(&self.undo_history_head.to_le_bytes());
        page[OFF_UNDO_HISTORY_TAIL..OFF_UNDO_HISTORY_TAIL + 8]
            .copy_from_slice(&self.undo_history_tail.to_le_bytes());
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
        let ver = u32::from_le_bytes(page[8..12].try_into().unwrap());
        if ver != VERSION_V3 {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unsupported meta version: {ver}"),
            )));
        }

        let root_page_id = u64::from_le_bytes(page[12..20].try_into().unwrap());
        let next_bptree_page_id = u64::from_le_bytes(page[20..28].try_into().unwrap());
        let next_data_page_id = u64::from_le_bytes(page[28..36].try_into().unwrap());
        let next_undo_page_id = u64::from_le_bytes(page[36..44].try_into().unwrap());

        let mut undo_free: Vec<PageId> = Vec::new();
        let n = u32::from_le_bytes(
            page[OFF_UNDO_FREE_LEN..OFF_UNDO_FREE_LEN + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let n = n.min(UNDO_FREE_CAP);
        let mut off = OFF_UNDO_FREE_IDS;
        for _ in 0..n {
            let pid = u64::from_le_bytes(page[off..off + 8].try_into().unwrap());
            if pid != 0 {
                undo_free.push(pid);
            }
            off += 8;
        }
        let undo_history_head = u64::from_le_bytes(
            page[OFF_UNDO_HISTORY_HEAD..OFF_UNDO_HISTORY_HEAD + 8]
                .try_into()
                .unwrap(),
        );
        let undo_history_tail = u64::from_le_bytes(
            page[OFF_UNDO_HISTORY_TAIL..OFF_UNDO_HISTORY_TAIL + 8]
                .try_into()
                .unwrap(),
        );

        Ok(Self {
            root_page_id,
            next_bptree_page_id,
            next_data_page_id,
            next_undo_page_id,
            undo_free,
            undo_history_head,
            undo_history_tail,
        })
    }

    pub fn pop_free_undo(&mut self) -> Option<PageId> {
        self.undo_free.pop()
    }

    pub fn push_free_undo(&mut self, pid: PageId) {
        if self.undo_free.len() < UNDO_FREE_CAP {
            self.undo_free.push(pid);
        }
    }
}
