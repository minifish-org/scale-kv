use crate::{Error, KEY_SIZE, VALUE_SIZE};
use std::collections::{BTreeMap, HashMap};
use tokio::sync::RwLock;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SecondaryIndexKind {
    Btree {
        value_offset: usize,
        value_len: usize,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondaryIndexDefinition {
    pub name: String,
    pub kind: SecondaryIndexKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondaryIndexMutation {
    pub index_name: String,
    pub secondary_key: Vec<u8>,
    pub primary_key: [u8; KEY_SIZE],
    pub present: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PostingVersion {
    commit_lsn: u64,
    present: bool,
}

#[derive(Default)]
struct SecondaryBtreeState {
    // secondary_key -> primary_key -> membership versions
    postings: BTreeMap<Vec<u8>, BTreeMap<[u8; KEY_SIZE], Vec<PostingVersion>>>,
}

struct SecondaryIndexState {
    def: SecondaryIndexDefinition,
    btree: SecondaryBtreeState,
}

#[derive(Default)]
pub struct SecondaryIndexManager {
    indexes: RwLock<HashMap<String, SecondaryIndexState>>,
}

impl SecondaryIndexManager {
    pub async fn replace_definitions(
        &self,
        defs: Vec<SecondaryIndexDefinition>,
    ) -> crate::Result<()> {
        let mut new_map: HashMap<String, SecondaryIndexState> = HashMap::new();
        for def in defs {
            if def.name.is_empty() {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "index name must be non-empty",
                )));
            }
            if new_map.contains_key(&def.name) {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("duplicate index name: {}", def.name),
                )));
            }
            let SecondaryIndexKind::Btree {
                value_offset,
                value_len,
            } = def.kind;
            if value_len == 0 || value_offset >= VALUE_SIZE || value_offset + value_len > VALUE_SIZE
            {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "secondary index range out of bounds: offset={} len={} value_size={}",
                        value_offset, value_len, VALUE_SIZE
                    ),
                )));
            }
            new_map.insert(
                def.name.clone(),
                SecondaryIndexState {
                    def,
                    btree: SecondaryBtreeState::default(),
                },
            );
        }
        let mut guard = self.indexes.write().await;
        *guard = new_map;
        Ok(())
    }

    pub async fn create_btree_index(
        &self,
        name: &str,
        value_offset: usize,
        value_len: usize,
    ) -> crate::Result<()> {
        if name.is_empty() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "index name must be non-empty",
            )));
        }
        if value_len == 0 {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "value_len must be > 0",
            )));
        }
        if value_offset >= VALUE_SIZE || value_offset + value_len > VALUE_SIZE {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "secondary index range out of bounds: offset={} len={} value_size={}",
                    value_offset, value_len, VALUE_SIZE
                ),
            )));
        }

        let def = SecondaryIndexDefinition {
            name: name.to_string(),
            kind: SecondaryIndexKind::Btree {
                value_offset,
                value_len,
            },
        };
        let mut guard = self.indexes.write().await;
        if guard.contains_key(name) {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("secondary index already exists: {}", name),
            )));
        }
        guard.insert(
            name.to_string(),
            SecondaryIndexState {
                def,
                btree: SecondaryBtreeState::default(),
            },
        );
        Ok(())
    }

    pub async fn drop_index(&self, name: &str) -> bool {
        self.indexes.write().await.remove(name).is_some()
    }

    pub async fn list_indexes(&self) -> Vec<SecondaryIndexDefinition> {
        let guard = self.indexes.read().await;
        let mut out = guard.values().map(|s| s.def.clone()).collect::<Vec<_>>();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    pub async fn plan_mutations(
        &self,
        primary_key: [u8; KEY_SIZE],
        old_value: Option<&[u8; VALUE_SIZE]>,
        new_value: Option<&[u8; VALUE_SIZE]>,
    ) -> crate::Result<Vec<SecondaryIndexMutation>> {
        let guard = self.indexes.read().await;
        let mut out = Vec::new();
        for state in guard.values() {
            match state.def.kind {
                SecondaryIndexKind::Btree {
                    value_offset,
                    value_len,
                } => {
                    let old_key =
                        old_value.map(|v| v[value_offset..value_offset + value_len].to_vec());
                    let new_key =
                        new_value.map(|v| v[value_offset..value_offset + value_len].to_vec());
                    if old_key == new_key {
                        continue;
                    }
                    if let Some(key) = old_key {
                        out.push(SecondaryIndexMutation {
                            index_name: state.def.name.clone(),
                            secondary_key: key,
                            primary_key,
                            present: false,
                        });
                    }
                    if let Some(key) = new_key {
                        out.push(SecondaryIndexMutation {
                            index_name: state.def.name.clone(),
                            secondary_key: key,
                            primary_key,
                            present: true,
                        });
                    }
                }
            }
        }
        Ok(out)
    }

    pub async fn apply_commit(&self, commit_lsn: u64, mutations: &[SecondaryIndexMutation]) {
        if mutations.is_empty() {
            return;
        }

        let mut guard = self.indexes.write().await;
        for m in mutations {
            let Some(state) = guard.get_mut(&m.index_name) else {
                continue;
            };
            let by_pk = state
                .btree
                .postings
                .entry(m.secondary_key.clone())
                .or_default();
            let history = by_pk.entry(m.primary_key).or_default();

            if let Some(last) = history.last_mut()
                && last.commit_lsn == commit_lsn
            {
                last.present = m.present;
                continue;
            }

            history.push(PostingVersion {
                commit_lsn,
                present: m.present,
            });
        }
    }

    pub async fn query_equal(
        &self,
        index_name: &str,
        secondary_key: &[u8],
        read_lsn: u64,
        limit: usize,
    ) -> crate::Result<Vec<[u8; KEY_SIZE]>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let guard = self.indexes.read().await;
        let Some(state) = guard.get(index_name) else {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("secondary index not found: {}", index_name),
            )));
        };

        let mut out = Vec::with_capacity(limit);
        let Some(by_pk) = state.btree.postings.get(secondary_key) else {
            return Ok(out);
        };
        for (pk, versions) in by_pk {
            if visible_membership(versions, read_lsn) {
                out.push(*pk);
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }
}

fn visible_membership(versions: &[PostingVersion], read_lsn: u64) -> bool {
    if versions.is_empty() {
        return false;
    }
    let idx = versions.partition_point(|v| v.commit_lsn <= read_lsn);
    if idx == 0 {
        return false;
    }
    versions[idx - 1].present
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_snapshot_visibility_on_membership_history() {
        let m = SecondaryIndexManager::default();
        m.create_btree_index("tag", 0, 2).await.unwrap();
        let pk = [7u8; KEY_SIZE];

        let mut v1 = [0u8; VALUE_SIZE];
        v1[0..2].copy_from_slice(b"aa");
        let mut v2 = [0u8; VALUE_SIZE];
        v2[0..2].copy_from_slice(b"bb");

        let muts = m.plan_mutations(pk, None, Some(&v1)).await.unwrap();
        m.apply_commit(10, &muts).await;
        let muts = m.plan_mutations(pk, Some(&v1), Some(&v2)).await.unwrap();
        m.apply_commit(20, &muts).await;

        let q10 = m.query_equal("tag", b"aa", 10, 10).await.unwrap();
        assert_eq!(q10, vec![pk]);
        let q15 = m.query_equal("tag", b"aa", 15, 10).await.unwrap();
        assert_eq!(q15, vec![pk]);
        let q20 = m.query_equal("tag", b"aa", 20, 10).await.unwrap();
        assert!(q20.is_empty());
        let q20b = m.query_equal("tag", b"bb", 20, 10).await.unwrap();
        assert_eq!(q20b, vec![pk]);
    }

    #[tokio::test]
    async fn test_same_commit_last_mutation_wins() {
        let m = SecondaryIndexManager::default();
        m.create_btree_index("tag", 0, 1).await.unwrap();
        let pk = [1u8; KEY_SIZE];
        m.apply_commit(
            42,
            &[
                SecondaryIndexMutation {
                    index_name: "tag".to_string(),
                    secondary_key: vec![1],
                    primary_key: pk,
                    present: true,
                },
                SecondaryIndexMutation {
                    index_name: "tag".to_string(),
                    secondary_key: vec![1],
                    primary_key: pk,
                    present: false,
                },
            ],
        )
        .await;
        let q = m.query_equal("tag", &[1], 42, 10).await.unwrap();
        assert!(q.is_empty());
    }

    #[tokio::test]
    async fn test_plan_mutations_noop_when_key_unchanged() {
        let m = SecondaryIndexManager::default();
        m.create_btree_index("ix", 4, 4).await.unwrap();
        let pk = [2u8; KEY_SIZE];
        let mut v = [0u8; VALUE_SIZE];
        v[4..8].copy_from_slice(&[1, 2, 3, 4]);
        let muts = m.plan_mutations(pk, Some(&v), Some(&v)).await.unwrap();
        assert!(muts.is_empty());
    }

    #[tokio::test]
    async fn test_create_and_drop_index() {
        let m = SecondaryIndexManager::default();
        m.create_btree_index("ix", 0, 1).await.unwrap();
        let defs = m.list_indexes().await;
        assert_eq!(defs.len(), 1);
        assert_eq!(defs[0].name, "ix");
        assert!(m.drop_index("ix").await);
        assert!(!m.drop_index("ix").await);
    }

    #[tokio::test]
    async fn test_query_returns_not_found_for_missing_index() {
        let m = SecondaryIndexManager::default();
        let err = m.query_equal("missing", b"x", 1, 10).await.unwrap_err();
        match err {
            Error::Io(ioe) => assert_eq!(ioe.kind(), std::io::ErrorKind::NotFound),
            other => panic!("unexpected error: {:?}", other),
        }
    }

    #[test]
    fn test_primary_key_size_is_fixed() {
        let _ = std::collections::BTreeSet::<[u8; KEY_SIZE]>::new();
    }
}
