use crate::PageId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DurableLsnResponse {
    pub durable_lsn: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageWrite {
    pub page_id: PageId,
    pub page: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendTxnBatchRequest {
    pub request_id: u64,
    pub start_lsn: u64,
    pub end_lsn: u64,
    pub writes: Vec<PageWrite>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendTxnBatchResponse {
    pub commit_lsn: u64,
    pub durable_lsn: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetPageResponse {
    pub durable_lsn: u64,
    pub found: bool,
    pub page_lsn: u64,
    pub page: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanPagesRequest {
    pub start_page_id: PageId,
    pub limit: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanPageItem {
    pub page_id: PageId,
    pub page_lsn: u64,
    pub page: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanPagesResponse {
    pub durable_lsn: u64,
    pub pages: Vec<ScanPageItem>,
}
