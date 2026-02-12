use scale_kv::{EmbeddedCompute, KEY_SIZE, StorageServer, VALUE_SIZE};
use std::net::SocketAddr;
use tokio::task::LocalSet;

fn tcp_bind_allowed() -> bool {
    std::net::TcpListener::bind("127.0.0.1:0").is_ok()
}

fn temp_dir() -> String {
    let mut p = std::env::temp_dir();
    let name = format!("scale-kv-gc-{}", std::process::id());
    p.push(name);
    // best-effort cleanup first
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p.to_string_lossy().to_string()
}

fn fixed_key(input: &[u8]) -> Vec<u8> {
    let mut out = vec![0u8; KEY_SIZE];
    let n = input.len().min(KEY_SIZE);
    out[..n].copy_from_slice(&input[..n]);
    out
}

#[tokio::test(flavor = "current_thread")]
async fn test_gc_respects_active_read_lsn() {
    if !tcp_bind_allowed() {
        return;
    }
    let dir = temp_dir();
    let local = LocalSet::new();
    local
        .run_until(async {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let server = StorageServer::start_with_dir(addr, dir.clone().into())
                .await
                .unwrap();

            let addrs = vec![server.addr().to_string()];
            let compute = EmbeddedCompute::connect(&addrs, 1, &local).await.unwrap();

            let key = fixed_key(b"k");
            let v1 = vec![b'a'; VALUE_SIZE];

            compute.put(&key, &v1).await.unwrap();

            // Hold a RO snapshot.
            let (_ro_lsn, guard) = compute.begin_ro_guard();

            // Delete after RO started.
            compute.delete(&key).await.unwrap();

            // GC should not physically remove key because gc_lsn is pinned by guard.
            compute.gc_once(1024).await.unwrap();

            // Key should still be logically deleted for new readers, but mapping may remain.
            assert_eq!(compute.get(&key).await.unwrap(), None);

            drop(guard);

            // Now GC can proceed.
            compute.gc_once(1024).await.unwrap();
            assert_eq!(compute.get(&key).await.unwrap(), None);

            // Ensure undo freelist is populated (best-effort).
            let meta_bytes = compute.cached_page(scale_kv::META_PAGE_ID).unwrap();
            let meta = scale_kv::MetaPage::decode(&meta_bytes).unwrap();
            assert!(meta.undo_free.len() > 0);
        })
        .await;

    let _ = std::fs::remove_dir_all(&dir);
}
