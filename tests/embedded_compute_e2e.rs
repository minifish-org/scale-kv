use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{fs, process};

use scale_kv::{EmbeddedCompute, PAGE_SIZE, StorageServer};

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn tcp_bind_allowed() -> bool {
    std::net::TcpListener::bind("127.0.0.1:0").is_ok()
}

fn temp_dir() -> PathBuf {
    let mut dir = std::env::temp_dir();
    let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    dir.push(format!(
        "scale-kv-embedded-page-redo-e2e-{}-{}",
        process::id(),
        id
    ));
    fs::create_dir_all(&dir).expect("failed to create temp dir");
    dir
}

fn cleanup_dir(dir: &PathBuf) {
    let _ = fs::remove_dir_all(dir);
}


#[tokio::test(flavor = "current_thread")]
async fn test_page_redo_commit_and_recover_by_scan() {
    if !tcp_bind_allowed() {
        return;
    }

    let dir = temp_dir();
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let server = StorageServer::start_with_dir(addr, dir.clone())
                .await
                .unwrap();

            let addrs = vec![server.addr().to_string()];
            let compute1 = EmbeddedCompute::connect(&addrs, 1, &local).await.unwrap();

            // Commit a single page after-image.
            let page_id = 42u64;
            let mut page = vec![0u8; PAGE_SIZE];
            page[0] = 7;
            let commit_lsn = compute1.write_page(page_id, page.clone()).await.unwrap();
            assert!(commit_lsn > 0);

            // New compute instance should be able to warm up by scanning pages.
            let compute2 = EmbeddedCompute::connect(&addrs, 1, &local).await.unwrap();
            let warmed = compute2.warmup_scan_all(256).await.unwrap();
            assert!(warmed >= 1);

            if compute2.cached_page(page_id).is_none() {
                // Debug: scan directly from storage
                let c = scale_kv::StorageClient::connect(&addrs[0], &local)
                    .await
                    .unwrap();
                let (pages, _) = c.scan_pages(0, 256).await.unwrap();
                panic!(
                    "page not warmed; scanPages returned ids: {:?}",
                    pages.iter().map(|(id, _, _)| *id).collect::<Vec<_>>()
                );
            }

            let got = compute2.cached_page(page_id).unwrap();
            assert_eq!(got, page);
        })
        .await;

    cleanup_dir(&dir);
}
