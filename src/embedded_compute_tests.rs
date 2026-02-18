use super::*;

fn fixed_key(seed: u8) -> Vec<u8> {
    let mut key = vec![0u8; KEY_SIZE];
    key[0] = seed;
    key
}

fn fixed_value(seed: u8) -> Vec<u8> {
    vec![seed; VALUE_SIZE]
}

#[test]
fn test_inprocess_sequencer_validation_and_commit() {
    let store = Arc::new(InProcessPageStore::default());
    let seq = InProcessSequencer::new(Arc::clone(&store));
    assert_eq!(seq.begin_ro(), 0);
    assert_eq!(seq.durable_lsn(), 0);

    let err = seq.reserve_txn_with_request_id(0, 1).unwrap_err();
    assert_eq!(err.category(), crate::ErrorCategory::InvalidInput);

    let err = seq.reserve_txn_with_request_id(1, 0).unwrap_err();
    assert_eq!(err.category(), crate::ErrorCategory::InvalidInput);

    let (request_id, start_lsn, end_lsn) = seq.reserve_txn(2).unwrap();
    assert!(request_id > 0);
    assert_eq!(start_lsn, 0);
    assert_eq!(end_lsn, 2);

    let page = Page::from(vec![7u8; PAGE_SIZE]);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let out = seq
            .commit_pages_reserved(request_id, start_lsn, end_lsn, vec![(42, page.clone())])
            .await
            .unwrap();
        assert_eq!(out, end_lsn);
    });

    assert_eq!(seq.durable_lsn(), end_lsn);
    assert_eq!(store.get(42), Some(page));
}

#[tokio::test(flavor = "current_thread")]
async fn test_connect_inprocess_validation_and_visibility() {
    let local = LocalSet::new();
    local
        .run_until(async {
            let compute = EmbeddedCompute::connect_inprocess(&local).await.unwrap();
            assert!(compute.warmed_pages() > 0);
            assert_eq!(
                compute.warmup_scan_all(0).await.unwrap(),
                compute.warmed_pages()
            );

            let short_key = [1u8; 3];
            let short_value = [2u8; 7];
            assert!(matches!(
                compute.put(&short_key, &fixed_value(1)).await.unwrap_err(),
                Error::InvalidKeySize(_, _)
            ));
            assert!(matches!(
                compute.put(&fixed_key(1), &short_value).await.unwrap_err(),
                Error::InvalidValueSize(_, _)
            ));
            assert!(matches!(
                compute.get(&short_key).await.unwrap_err(),
                Error::InvalidKeySize(_, _)
            ));
            assert!(matches!(
                compute.delete(&short_key).await.unwrap_err(),
                Error::InvalidKeySize(_, _)
            ));

            let k1 = fixed_key(11);
            let k2 = fixed_key(12);
            let v1 = fixed_value(1);
            let v2 = fixed_value(2);
            compute.put(&k1, &v1).await.unwrap();
            compute.put(&k2, &v2).await.unwrap();

            assert!(compute.get_exists(&k1).await.unwrap());
            assert_eq!(
                compute.scan_range_exists_count(&k1, &k2, 0).await.unwrap(),
                0
            );
            assert!(
                compute
                    .scan_secondary_index_eq("missing", b"x", 0)
                    .await
                    .unwrap()
                    .is_empty()
            );

            compute.delete(&k1).await.unwrap();
            assert_eq!(compute.get(&k1).await.unwrap(), None);
            assert!(!compute.get_exists(&k1).await.unwrap());
            assert_eq!(
                compute.scan_range_exists_count(&k1, &k2, 10).await.unwrap(),
                1
            );
            assert_eq!(compute.gc_once(0).await.unwrap(), 0);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_read_only_txn_write_is_rejected() {
    let local = LocalSet::new();
    local
        .run_until(async {
            let compute = EmbeddedCompute::connect_inprocess(&local).await.unwrap();

            let mut ro = compute.begin_ro_timeout(Duration::from_secs(1));
            ro.write_page(100, Page::from(vec![1u8; PAGE_SIZE]));
            let err = ro.commit().await.unwrap_err();
            assert!(matches!(err, Error::Io(_)));

            let mut ro2 = compute.begin_ro_timeout(Duration::from_secs(1));
            let err = ro2.put(&fixed_key(1), &fixed_value(1)).await.unwrap_err();
            assert!(matches!(err, Error::Io(_)));
            ro2.abort().await.unwrap();
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_rw_txn_timeout_enforced() {
    let local = LocalSet::new();
    local
        .run_until(async {
            let compute = EmbeddedCompute::connect_inprocess(&local).await.unwrap();
            let mut tx = compute.begin_rw_timeout(Duration::from_millis(15)).await;
            tokio::time::sleep(Duration::from_millis(40)).await;
            let err = tx.put(&fixed_key(9), &fixed_value(9)).await.unwrap_err();
            assert!(matches!(err, Error::TxnTimeout));
            assert_eq!(err.category(), crate::ErrorCategory::Timeout);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn test_secondary_index_invalid_definition_and_query_errors() {
    let local = LocalSet::new();
    local
        .run_until(async {
            let compute = EmbeddedCompute::connect_inprocess(&local).await.unwrap();

            let err = compute
                .create_btree_secondary_index("bad", VALUE_SIZE, 8)
                .await
                .unwrap_err();
            assert_eq!(err.category(), crate::ErrorCategory::InvalidInput);

            compute
                .create_btree_secondary_index("tag", 0, 2)
                .await
                .unwrap();
            let dup_err = compute
                .create_btree_secondary_index("tag", 0, 2)
                .await
                .unwrap_err();
            assert_eq!(dup_err.category(), crate::ErrorCategory::InvalidInput);

            let miss_err = compute
                .scan_secondary_index_eq("not_found", b"aa", 10)
                .await
                .unwrap_err();
            assert_eq!(miss_err.category(), crate::ErrorCategory::InvalidInput);
        })
        .await;
}
