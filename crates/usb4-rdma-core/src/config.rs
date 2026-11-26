use crate::{Error, NodeId, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Node {
    pub id: NodeId,
    pub name: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Link {
    pub id: String,
    pub a: NodeId,
    pub b: NodeId,
    pub endpoint_a: String,
    pub endpoint_b: String,
    pub controller_a: String,
    pub controller_b: String,
    #[serde(default = "default_speed")]
    pub speed_mbps: u32,
}

fn default_speed() -> u32 {
    40000
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Cluster {
    pub epoch: u64,
    pub nodes: Vec<Node>,
    pub links: Vec<Link>,
    #[serde(default = "default_queue_depth")]
    pub queue_depth: usize,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
}

fn default_queue_depth() -> usize {
    64
}

fn default_timeout_ms() -> u64 {
    10000
}

impl Cluster {
    pub fn parse(s: &str) -> Result<Self> {
        let c: Self = toml::from_str(s).map_err(|e| Error::Config(e.to_string()))?;
        c.validate()?;
        Ok(c)
    }

    pub fn load(path: &std::path::Path) -> Result<Self> {
        Self::parse(&std::fs::read_to_string(path)?)
    }

    pub fn validate(&self) -> Result<()> {
        if self.nodes.len() < 2
            || self.nodes.len() > 64
            || self.epoch == 0
            || !(2..=256).contains(&self.queue_depth)
            || self.timeout_ms == 0
        {
            return Err(Error::Config(
                "nodes=2..64, epoch>0, queue=2..256, timeout>0 required".into(),
            ));
        }
        let ids: BTreeSet<_> = self.nodes.iter().map(|n| n.id).collect();
        if ids.len() != self.nodes.len() {
            return Err(Error::Config("duplicate node".into()));
        }
        let mut links = BTreeSet::new();
        for l in &self.links {
            if !ids.contains(&l.a)
                || !ids.contains(&l.b)
                || l.a == l.b
                || !links.insert(&l.id)
                || l.speed_mbps == 0
            {
                return Err(Error::Config("invalid/duplicate link".into()));
            }
            for e in [&l.endpoint_a, &l.endpoint_b] {
                if e.is_empty() {
                    return Err(Error::Config("empty endpoint".into()));
                }
            }
        }
        let adj = self.adjacency();
        let root = *ids.first().expect("nodes");
        let mut reached = BTreeSet::from([root]);
        let mut q = VecDeque::from([root]);
        while let Some(n) = q.pop_front() {
            for &p in &adj[&n] {
                if reached.insert(p) {
                    q.push_back(p);
                }
            }
        }
        if reached != ids {
            return Err(Error::Config("disconnected graph".into()));
        }
        Ok(())
    }

    pub fn adjacency(&self) -> BTreeMap<NodeId, BTreeSet<NodeId>> {
        let mut a: BTreeMap<_, _> = self.nodes.iter().map(|n| (n.id, BTreeSet::new())).collect();
        for l in &self.links {
            a.entry(l.a).or_default().insert(l.b);
            a.entry(l.b).or_default().insert(l.a);
        }
        a
    }

    pub fn digest(&self) -> [u8; 32] {
        let mut c = self.clone();
        c.nodes.sort_by_key(|n| n.id);
        c.links.sort_by(|a, b| a.id.cmp(&b.id));
        Sha256::digest(toml::to_string(&c).expect("serializable cluster")).into()
    }

    pub fn node(&self, id: NodeId) -> Result<&Node> {
        self.nodes
            .iter()
            .find(|n| n.id == id)
            .ok_or_else(|| Error::Config("unknown node".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_disconnected_graph() {
        let t = r#"
epoch = 1
[[nodes]]
id = 0
name = "n0"
[[nodes]]
id = 1
name = "n1"
[[nodes]]
id = 2
name = "n2"
[[links]]
id = "l0"
a = 0
b = 1
endpoint_a = "e0"
endpoint_b = "e1"
controller_a = "c0"
controller_b = "c1"
"#;
        assert!(Cluster::parse(t).is_err());
    }
}
