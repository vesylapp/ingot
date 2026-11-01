//! In-memory cache of active functions, indexed for trigger lookup.

use crate::function::event_patterns;
use crate::store::MetaStore;
use crate::types::Function;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use uuid::Uuid;

#[derive(Default)]
struct Index {
    by_id: HashMap<Uuid, Arc<Function>>,
    by_slug: HashMap<String, Arc<Function>>,
    /// Trigger event pattern -> functions with that trigger.
    by_trigger: HashMap<String, Vec<Arc<Function>>>,
}

pub struct Registry {
    meta: Arc<dyn MetaStore>,
    index: RwLock<Arc<Index>>,
    generation: AtomicI64,
}

impl Registry {
    pub fn new(meta: Arc<dyn MetaStore>) -> Registry {
        Registry {
            meta,
            index: RwLock::new(Arc::new(Index::default())),
            generation: AtomicI64::new(-1),
        }
    }

    pub async fn reload(&self) -> anyhow::Result<()> {
        let generation = self.meta.functions_generation().await?;
        let fns = self.meta.list_functions().await?;
        let mut idx = Index::default();
        for f in fns {
            let f = Arc::new(f);
            idx.by_id.insert(f.id, f.clone());
            idx.by_slug.insert(f.slug.clone(), f.clone());
            for t in &f.triggers {
                if let Some(ev) = &t.event {
                    let list = idx.by_trigger.entry(ev.clone()).or_default();
                    if !list.iter().any(|x| x.id == f.id) {
                        list.push(f.clone());
                    }
                }
            }
        }
        *self.index.write() = Arc::new(idx);
        self.generation.store(generation, Ordering::SeqCst);
        Ok(())
    }

    /// Reload if another node (or this one) synced functions since the last load.
    pub async fn refresh_if_changed(&self) -> anyhow::Result<bool> {
        let g = self.meta.functions_generation().await?;
        if g != self.generation.load(Ordering::SeqCst) {
            self.reload().await?;
            return Ok(true);
        }
        Ok(false)
    }

    pub fn get(&self, id: Uuid) -> Option<Arc<Function>> {
        self.index.read().by_id.get(&id).cloned()
    }

    pub fn by_slug(&self, slug: &str) -> Option<Arc<Function>> {
        self.index.read().by_slug.get(slug).cloned()
    }

    pub fn len(&self) -> usize {
        self.index.read().by_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn all(&self) -> Vec<Arc<Function>> {
        self.index.read().by_id.values().cloned().collect()
    }

    /// Functions with a trigger matching the event name (exact or wildcard).
    pub fn for_event(&self, name: &str) -> Vec<Arc<Function>> {
        let idx = self.index.read().clone();
        let mut out: Vec<Arc<Function>> = Vec::new();
        for p in event_patterns(name) {
            if let Some(list) = idx.by_trigger.get(&p) {
                for f in list {
                    if !out.iter().any(|x| x.id == f.id) {
                        out.push(f.clone());
                    }
                }
            }
        }
        out
    }
}
