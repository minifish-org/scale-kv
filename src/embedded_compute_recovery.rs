use super::*;

impl EmbeddedCompute {
    pub(super) async fn persist_secondary_index_catalog(&self) -> Result<()> {
        let defs = self.secondary_indexes.list_indexes().await;
        let posting_log = { self.secondary_posting_log.lock().await.clone() };
        let page = encode_secondary_index_catalog(&SecondaryIndexCatalog { defs, posting_log })?;
        let _ = self
            .write_pages_direct(vec![(SECONDARY_INDEX_META_PAGE_ID, page)])
            .await?;
        Ok(())
    }

    pub(super) async fn load_secondary_index_catalog(&self) -> Result<()> {
        let Some(page) = self
            .get_page(SECONDARY_INDEX_META_PAGE_ID, self.durable_lsn())
            .await
        else {
            self.secondary_indexes
                .replace_definitions(Vec::new())
                .await?;
            *self.secondary_posting_log.lock().await = SecondaryPostingLogState::default();
            return Ok(());
        };
        let catalog = decode_secondary_index_catalog(&page)?;
        self.secondary_indexes
            .replace_definitions(catalog.defs.clone())
            .await?;
        *self.secondary_posting_log.lock().await = catalog.posting_log;
        Ok(())
    }

    pub(super) async fn append_secondary_posting_log(
        &self,
        commit_lsn: u64,
        mutations: &[SecondaryIndexMutation],
    ) -> Result<()> {
        if mutations.is_empty() {
            return Ok(());
        }

        let mut state = self.secondary_posting_log.lock().await;
        let mut writes: BTreeMap<PageId, Vec<u8>> = BTreeMap::new();
        let mut tail_page_id = state.tail_page_id;
        let mut tail_page = if tail_page_id == 0 {
            let new_id = state.next_page_id;
            state.next_page_id = state.next_page_id.saturating_add(1);
            state.head_page_id = new_id;
            state.tail_page_id = new_id;
            tail_page_id = new_id;
            new_log_page().to_vec()
        } else if let Some(existing) = self.page_cache.get(tail_page_id) {
            existing.to_vec()
        } else {
            self.get_page(tail_page_id, self.durable_lsn())
                .await
                .ok_or(Error::InMemoryPageMissing(tail_page_id))?
                .to_vec()
        };

        for m in mutations {
            if append_posting_record(&mut tail_page, commit_lsn, m)? {
                continue;
            }

            let new_id = state.next_page_id;
            state.next_page_id = state.next_page_id.saturating_add(1);
            write_posting_next_page_id(&mut tail_page, new_id)?;
            writes.insert(tail_page_id, tail_page);

            tail_page = new_log_page().to_vec();
            if !append_posting_record(&mut tail_page, commit_lsn, m)? {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "posting mutation record too large",
                )));
            }
            tail_page_id = new_id;
            state.tail_page_id = new_id;
        }

        writes.insert(tail_page_id, tail_page);
        let mut page_writes = writes
            .into_iter()
            .map(|(pid, p)| (pid, Page::from(p)))
            .collect::<Vec<_>>();
        let defs = self.secondary_indexes.list_indexes().await;
        let page = encode_secondary_index_catalog(&SecondaryIndexCatalog {
            defs,
            posting_log: state.clone(),
        })?;
        page_writes.push((SECONDARY_INDEX_META_PAGE_ID, page));
        let _ = self.write_pages_direct(page_writes).await?;
        Ok(())
    }

    pub(super) async fn replay_secondary_posting_log(&self) -> Result<usize> {
        let state = self.secondary_posting_log.lock().await.clone();
        if state.head_page_id == 0 {
            return Ok(0);
        }

        let mut count = 0usize;
        let mut page_id = state.head_page_id;
        while page_id != 0 {
            let page = self
                .get_page(page_id, self.durable_lsn())
                .await
                .ok_or(Error::InMemoryPageMissing(page_id))?;
            let records = decode_posting_records(&page)?;
            for (lsn, m) in records {
                self.secondary_indexes.apply_commit(lsn, &[m]).await;
                count += 1;
            }
            page_id = read_next_page_id(&page)?;
        }
        Ok(count)
    }

    pub(super) async fn backfill_secondary_index(&self, index_name: &str) -> Result<usize> {
        let mut tx = self.begin_tx(true, None);
        let start = vec![0u8; KEY_SIZE];
        let end = vec![0xFFu8; KEY_SIZE];
        let entries = tx.tree_range(&start, &end).await;

        let mut out_count = 0usize;
        let commit_lsn = self.durable_lsn();
        let mut chunk: Vec<SecondaryIndexMutation> = Vec::with_capacity(1024);

        for (key, row) in entries {
            if let Some(value) = tx.read_visible_value_from_scan_entry(&key, row).await? {
                let key_arr: [u8; KEY_SIZE] = key
                    .as_slice()
                    .try_into()
                    .map_err(|_| Error::InvalidKeySize(key.len(), KEY_SIZE))?;
                let mut value_arr = [0u8; VALUE_SIZE];
                value_arr.copy_from_slice(&value);
                let muts = self
                    .secondary_indexes
                    .plan_mutations(key_arr, None, Some(&value_arr))
                    .await?;
                for m in muts {
                    if m.index_name == index_name {
                        chunk.push(m);
                    }
                }
                out_count += 1;
                if chunk.len() >= 1024 {
                    self.secondary_indexes
                        .apply_commit(commit_lsn, &chunk)
                        .await;
                    self.append_secondary_posting_log(commit_lsn, &chunk)
                        .await?;
                    chunk.clear();
                }
            }
        }
        if !chunk.is_empty() {
            self.secondary_indexes
                .apply_commit(commit_lsn, &chunk)
                .await;
            self.append_secondary_posting_log(commit_lsn, &chunk)
                .await?;
        }
        Ok(out_count)
    }

    pub(super) async fn backfill_all_secondary_indexes(&self) -> Result<()> {
        let defs = self.secondary_indexes.list_indexes().await;
        for d in defs {
            self.backfill_secondary_index(&d.name).await?;
        }
        Ok(())
    }

    pub(super) async fn recover_or_init(&self) -> Result<()> {
        // Warmup first (best-effort). If storage is empty, we'll init below.
        let _ = self.warmup_scan_all(256).await;

        if let Some(global_meta_bytes) = self.page_cache.get(META_PAGE_ID) {
            let mut meta = MetaPage::decode(&global_meta_bytes)?;
            self.provider.set_next_page_id(meta.next_bptree_page_id);

            // Backward-compat guard: older meta pages may have next_undo_page_id=0.
            if meta.next_undo_page_id < 2_000_000 {
                meta.next_undo_page_id = 2_000_000;
                // best-effort persist
                let _ = self.write_page(META_PAGE_ID, meta.encode()).await;
            }

            // Prefer btree metapage for root.
            if let Some(btree_meta_bytes) = self.page_cache.get(BTREE_META_PAGE_ID) {
                let bm = BtreeMeta::decode(&btree_meta_bytes)?;
                self.provider.set_root_page_id(bm.root_page_id);
            } else {
                // Fallback: root in global meta.
                self.provider.set_root_page_id(meta.root_page_id);
            }
            self.load_secondary_index_catalog().await?;
            let replayed = self.replay_secondary_posting_log().await?;
            if replayed == 0 {
                self.backfill_all_secondary_indexes().await?;
            }
            return Ok(());
        }

        // Cold start: create meta + root page as one txn.
        // PageId plan:
        // - 0: meta
        // - 10.. : bptree pages
        // - 2_000_000.. : undo pages

        const BPTREE_ROOT_ID: PageId = 10;
        const UNDO_BASE: PageId = 2_000_000;

        let mut tx = self.begin_rw().await;

        // Configure provider root + next_page_id (next after reserved ids).
        self.provider.set_root_page_id(BPTREE_ROOT_ID);
        self.provider.set_next_page_id(BPTREE_ROOT_ID + 1);

        // Root leaf page.
        let root_page = crate::page_bptree::new_page(2, 0);
        tx.write_page(BPTREE_ROOT_ID, root_page);

        // B-Tree metapage.
        tx.write_page(
            BTREE_META_PAGE_ID,
            BtreeMeta {
                root_page_id: BPTREE_ROOT_ID,
            }
            .encode(),
        );

        // Meta page.
        let meta = MetaPage {
            root_page_id: BPTREE_ROOT_ID,
            next_bptree_page_id: BPTREE_ROOT_ID + 1,
            next_data_page_id: 1_000_000,
            next_undo_page_id: UNDO_BASE,
            undo_free: Vec::new(),
            undo_history_head: 0,
            undo_history_tail: 0,
        };
        tx.write_page(META_PAGE_ID, meta.encode());

        tx.commit().await?;
        self.secondary_indexes
            .replace_definitions(Vec::new())
            .await?;
        self.persist_secondary_index_catalog().await?;
        Ok(())
    }
}
