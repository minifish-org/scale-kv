use crate::http_protocol::{
    AppendTxnBatchRequest, AppendTxnBatchResponse, DurableLsnResponse, GetPageResponse,
    ScanPageItem, ScanPagesRequest, ScanPagesResponse,
};
use crate::{Page, StorageMaintenanceConfig, StorageNode};
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tower_http::cors::CorsLayer;

#[derive(Clone)]
struct AppState {
    data: Arc<StorageNode>,
}

pub struct StorageServer {
    data: Arc<StorageNode>,
    addr: SocketAddr,
}

impl StorageServer {
    pub async fn start(addr: SocketAddr) -> crate::Result<Self> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let addr = listener.local_addr()?;
        let data = Arc::new(StorageNode::new().await);
        let app = build_router(data.clone());
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self { data, addr })
    }

    pub async fn start_with_dir(addr: SocketAddr, dir: PathBuf) -> crate::Result<Self> {
        Self::start_with_dir_and_maintenance(addr, dir, StorageMaintenanceConfig::default()).await
    }

    pub async fn start_with_dir_and_maintenance(
        addr: SocketAddr,
        dir: PathBuf,
        maintenance: StorageMaintenanceConfig,
    ) -> crate::Result<Self> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let addr = listener.local_addr()?;
        let data = Arc::new(StorageNode::open_with_maintenance(dir, maintenance).await?);
        let app = build_router(data.clone());
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self { data, addr })
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn data(&self) -> &StorageNode {
        &self.data
    }

    pub fn data_arc(&self) -> Arc<StorageNode> {
        Arc::clone(&self.data)
    }
}

fn build_router(data: Arc<StorageNode>) -> Router {
    let state = AppState { data };
    Router::new()
        .route("/v1/storage/durable_lsn", get(get_durable_lsn))
        .route("/v1/storage/append_txn_batch", post(append_txn_batch))
        .route("/v1/storage/page/{page_id}", get(get_page))
        .route("/v1/storage/scan_pages", post(scan_pages))
        .layer(DefaultBodyLimit::max(32 * 1024 * 1024))
        .layer(CorsLayer::permissive())
        .with_state(state)
}

async fn get_durable_lsn(State(state): State<AppState>) -> Json<DurableLsnResponse> {
    Json(DurableLsnResponse {
        durable_lsn: state.data.durable_lsn(),
    })
}

async fn append_txn_batch(
    State(state): State<AppState>,
    Json(req): Json<AppendTxnBatchRequest>,
) -> std::result::Result<Json<AppendTxnBatchResponse>, (axum::http::StatusCode, String)> {
    let writes: Vec<(u64, Page)> = req
        .writes
        .into_iter()
        .map(|w| (w.page_id, Page::from(w.page)))
        .collect();

    let commit_lsn = match state
        .data
        .append_txn_batch_with_lsn_sync(req.request_id, req.start_lsn, req.end_lsn, writes)
        .await
    {
        Ok(v) => v,
        Err(err) => return Err(internal_error(err)),
    };

    Ok(Json(AppendTxnBatchResponse {
        commit_lsn,
        durable_lsn: state.data.durable_lsn(),
    }))
}

async fn get_page(
    State(state): State<AppState>,
    Path(page_id): Path<u64>,
) -> Json<GetPageResponse> {
    if let Some((page, page_lsn)) = state.data.get_page_latest(page_id).await {
        return Json(GetPageResponse {
            durable_lsn: state.data.durable_lsn(),
            found: true,
            page_lsn,
            page: page.to_vec(),
        });
    }
    Json(GetPageResponse {
        durable_lsn: state.data.durable_lsn(),
        found: false,
        page_lsn: 0,
        page: Vec::new(),
    })
}

async fn scan_pages(
    State(state): State<AppState>,
    Json(req): Json<ScanPagesRequest>,
) -> Json<ScanPagesResponse> {
    let pages = state
        .data
        .scan_pages_latest(req.start_page_id, req.limit as usize)
        .await
        .into_iter()
        .map(|(page_id, page_lsn, page)| ScanPageItem {
            page_id,
            page_lsn,
            page: page.to_vec(),
        })
        .collect();
    Json(ScanPagesResponse {
        durable_lsn: state.data.durable_lsn(),
        pages,
    })
}

fn internal_error(err: crate::Error) -> (axum::http::StatusCode, String) {
    (axum::http::StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}
