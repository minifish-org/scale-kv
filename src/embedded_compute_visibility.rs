use super::*;

impl EmbeddedTxn {
    pub(super) async fn read_visible_from_undo(
        &mut self,
        mut undo: Option<UndoPtr>,
    ) -> Result<Option<Bytes>> {
        while let Some(ptr) = undo {
            let upage = self
                .get_page_for_read(ptr.page_id)
                .await
                .ok_or(Error::InMemoryPageMissing(ptr.page_id))?;
            let rec = undo_pg::read_record_ref(&upage, ptr.slot_id)?;
            if (rec.old_flags & FLAG_INTENT) != 0 {
                undo = rec.prev;
                continue;
            }
            if rec.old_commit_lsn <= self.read_lsn {
                if (rec.old_flags & FLAG_TOMBSTONE) != 0 {
                    return Ok(None);
                }
                return Ok(Some(rec.old_value));
            }
            undo = rec.prev;
        }
        Ok(None)
    }

    pub(super) async fn read_visible_exists_from_undo(
        &mut self,
        mut undo: Option<UndoPtr>,
    ) -> Result<bool> {
        while let Some(ptr) = undo {
            let upage = self
                .get_page_for_read(ptr.page_id)
                .await
                .ok_or(Error::InMemoryPageMissing(ptr.page_id))?;
            let rec = undo_pg::read_record_ref(&upage, ptr.slot_id)?;
            if (rec.old_flags & FLAG_INTENT) != 0 {
                undo = rec.prev;
                continue;
            }
            if rec.old_commit_lsn <= self.read_lsn {
                return Ok((rec.old_flags & FLAG_TOMBSTONE) == 0);
            }
            undo = rec.prev;
        }
        Ok(false)
    }

    pub(super) async fn read_visible_value_from_row(
        &mut self,
        key: &[u8],
    ) -> Result<Option<Bytes>> {
        let intent_wait_grace = Duration::from_millis(50);
        loop {
            self.ensure_not_timed_out()?;

            let row = self.tree_get(key).await;
            let Some(row) = row else {
                return Ok(None);
            };

            if (row.meta.flags & FLAG_INTENT) != 0 {
                if self.undo_txn_id == Some(row.meta.intent_txn_id) {
                    if (row.meta.flags & FLAG_TOMBSTONE) != 0 {
                        return Ok(None);
                    }
                    return Ok(Some(row.value));
                }
                if row.meta.intent_lsn > self.read_lsn {
                    return self.read_visible_from_undo(row.meta.undo_ptr).await;
                }
                self.compute
                    .wait_on_pending_txn(row.meta.intent_txn_id, intent_wait_grace)
                    .await?;
                continue;
            }

            if row.meta.commit_lsn <= self.read_lsn {
                if (row.meta.flags & FLAG_TOMBSTONE) != 0 {
                    return Ok(None);
                }
                return Ok(Some(row.value));
            }

            // Not visible at this snapshot: walk undo chain to find latest visible version.
            return self.read_visible_from_undo(row.meta.undo_ptr).await;
        }
    }

    pub(super) async fn read_visible_exists_from_row(&mut self, key: &[u8]) -> Result<bool> {
        let intent_wait_grace = Duration::from_millis(50);
        loop {
            self.ensure_not_timed_out()?;

            let row = self.tree_get(key).await;
            let Some(row) = row else {
                return Ok(false);
            };

            if (row.meta.flags & FLAG_INTENT) != 0 {
                if self.undo_txn_id == Some(row.meta.intent_txn_id) {
                    return Ok((row.meta.flags & FLAG_TOMBSTONE) == 0);
                }
                if row.meta.intent_lsn > self.read_lsn {
                    return self.read_visible_exists_from_undo(row.meta.undo_ptr).await;
                }
                self.compute
                    .wait_on_pending_txn(row.meta.intent_txn_id, intent_wait_grace)
                    .await?;
                continue;
            }

            if row.meta.commit_lsn <= self.read_lsn {
                return Ok((row.meta.flags & FLAG_TOMBSTONE) == 0);
            }

            // Not visible at this snapshot: walk undo chain to find latest visible version.
            return self.read_visible_exists_from_undo(row.meta.undo_ptr).await;
        }
    }

    pub(super) async fn read_visible_exists_from_scan_entry(
        &mut self,
        key: &[u8],
        row: LeafValueMeta,
    ) -> Result<bool> {
        if (row.flags & FLAG_INTENT) != 0 {
            if self.undo_txn_id == Some(row.intent_txn_id) {
                return Ok((row.flags & FLAG_TOMBSTONE) == 0);
            }
            if row.intent_lsn > self.read_lsn {
                return self.read_visible_exists_from_undo(row.undo_ptr).await;
            }
            self.compute
                .wait_on_pending_txn(row.intent_txn_id, Duration::from_millis(50))
                .await?;
            return self.read_visible_exists_from_row(key).await;
        }
        if row.commit_lsn <= self.read_lsn {
            return Ok((row.flags & FLAG_TOMBSTONE) == 0);
        }
        self.read_visible_exists_from_undo(row.undo_ptr).await
    }

    pub(super) async fn read_visible_value_from_scan_entry(
        &mut self,
        key: &[u8],
        row: LeafValueRef,
    ) -> Result<Option<Bytes>> {
        if (row.meta.flags & FLAG_INTENT) != 0 {
            if self.undo_txn_id == Some(row.meta.intent_txn_id) {
                if (row.meta.flags & FLAG_TOMBSTONE) != 0 {
                    return Ok(None);
                }
                return Ok(Some(row.value));
            }
            if row.meta.intent_lsn > self.read_lsn {
                return self.read_visible_from_undo(row.meta.undo_ptr).await;
            }
            self.compute
                .wait_on_pending_txn(row.meta.intent_txn_id, Duration::from_millis(50))
                .await?;
            return self.read_visible_value_from_row(key).await;
        }
        if row.meta.commit_lsn <= self.read_lsn {
            if (row.meta.flags & FLAG_TOMBSTONE) != 0 {
                return Ok(None);
            }
            return Ok(Some(row.value));
        }
        self.read_visible_from_undo(row.meta.undo_ptr).await
    }
}
