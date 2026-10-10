//! Pure complete-history register. Parents must already exist; absent dependencies are not inferred.
use crate::{OpId, Operation};
use std::collections::{BTreeMap, BTreeSet};
#[derive(Clone, Debug, Default)]
pub struct Register {
    versions: BTreeMap<OpId, Operation>,
}
impl Register {
    pub fn insert(&mut self, op: Operation) -> Result<(), &'static str> {
        if let Some(old) = self
            .versions
            .values()
            .find(|v| v.id.identity_key() == op.id.identity_key())
        {
            return if old == &op {
                Ok(())
            } else {
                Err("operation fork")
            };
        }
        if self
            .versions
            .values()
            .any(|v| (v.table, &v.key) != (op.table, &op.key))
        {
            return Err("record scope");
        }
        for parent in &op.parents {
            let id = OpId::decode(parent)?;
            let p = self.versions.get(&id).ok_or("missing parent")?;
            if (p.table, &p.key) != (op.table, &op.key) {
                return Err("parent scope");
            }
        }
        self.versions.insert(op.id.clone(), op);
        Ok(())
    }
    pub fn heads(&self) -> Vec<Operation> {
        let covered: BTreeSet<Vec<u8>> = self
            .versions
            .values()
            .flat_map(|o| o.parents.iter().cloned())
            .collect();
        self.versions
            .values()
            .filter(|o| !covered.contains(&o.id.encode()))
            .cloned()
            .collect()
    }
    pub fn select_projection(mut heads: Vec<Operation>) -> Option<Operation> {
        heads.sort_by(|a, b| a.id.cmp(&b.id));
        heads.pop()
    }
    /// Largest parsed (dataset, device, incarnation, numeric seq, numeric index, schema).
    pub fn projection(&self) -> Option<Operation> {
        self.heads().pop()
    }
}
