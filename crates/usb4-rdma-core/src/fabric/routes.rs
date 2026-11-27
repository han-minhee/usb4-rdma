use crate::{Error, NodeId, Result, config::Cluster};
use std::collections::{BTreeMap, VecDeque};

#[derive(Clone)]
pub struct Routes {
    paths: BTreeMap<(NodeId, NodeId), Vec<NodeId>>,
    pub digest: [u8; 32],
    pub epoch: u64,
}

impl Routes {
    pub fn new(c: &Cluster) -> Result<Self> {
        c.validate()?;
        let adj = c.adjacency();
        let root = *adj.keys().next().unwrap();
        let mut parent = BTreeMap::from([(root, root)]);
        let mut q = VecDeque::from([root]);
        while let Some(n) = q.pop_front() {
            for &p in &adj[&n] {
                if let std::collections::btree_map::Entry::Vacant(e) = parent.entry(p) {
                    e.insert(n);
                    q.push_back(p);
                }
            }
        }
        let mut paths = BTreeMap::new();
        for &a in adj.keys() {
            for &b in adj.keys() {
                let path = if a == b {
                    vec![a]
                } else if adj[&a].contains(&b) {
                    vec![a, b]
                } else {
                    let ancestors = |mut n| {
                        let mut v = vec![n];
                        while n != root {
                            n = parent[&n];
                            v.push(n);
                        }
                        v
                    };
                    let av = ancestors(a);
                    let bv = ancestors(b);
                    let (ai, bi) = av
                        .iter()
                        .enumerate()
                        .find_map(|(i, n)| bv.iter().position(|x| x == n).map(|j| (i, j)))
                        .unwrap();
                    let mut p = av[..=ai].to_vec();
                    p.extend(bv[..bi].iter().rev());
                    p
                };
                paths.insert((a, b), path);
            }
        }
        Ok(Self {
            paths,
            digest: c.digest(),
            epoch: c.epoch,
        })
    }

    pub fn path(&self, source: NodeId, destination: NodeId) -> Result<&[NodeId]> {
        self.paths
            .get(&(source, destination))
            .map(Vec::as_slice)
            .ok_or_else(|| Error::Config("unknown route".into()))
    }

    pub fn next(
        &self,
        source: NodeId,
        destination: NodeId,
        current: NodeId,
    ) -> Result<Option<NodeId>> {
        let p = self.path(source, destination)?;
        let i = p
            .iter()
            .position(|&n| n == current)
            .ok_or(Error::Protocol("node outside frozen route"))?;
        Ok(p.get(i + 1).copied())
    }

    pub fn verify_previous(
        &self,
        source: NodeId,
        destination: NodeId,
        current: NodeId,
        previous: NodeId,
    ) -> Result<()> {
        let p = self.path(source, destination)?;
        let i = p
            .iter()
            .position(|&n| n == current)
            .ok_or(Error::Protocol("wrong route"))?;
        if i == 0 || p[i - 1] != previous {
            return Err(Error::Protocol("unexpected previous hop"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Link, Node};

    pub fn cluster(edges: &[(u32, u32)], n: u32) -> Cluster {
        Cluster {
            epoch: 9,
            nodes: (0..n)
                .map(|id| Node {
                    id,
                    name: id.to_string(),
                })
                .collect(),
            links: edges
                .iter()
                .enumerate()
                .map(|(i, &(a, b))| Link {
                    id: i.to_string(),
                    a,
                    b,
                    endpoint_a: "a".into(),
                    endpoint_b: "b".into(),
                    controller_a: "a".into(),
                    controller_b: "b".into(),
                    speed_mbps: 40000,
                })
                .collect(),
            queue_depth: 4,
            timeout_ms: 1000,
        }
    }

    #[test]
    fn direct_and_tree() {
        let c = cluster(&[(0, 1), (1, 2), (2, 3), (3, 0)], 4);
        let r = Routes::new(&c).unwrap();
        assert_eq!(r.path(0, 3).unwrap(), [0, 3]);
        assert_eq!(r.path(1, 3).unwrap(), [1, 0, 3]);
        assert!(r.verify_previous(1, 3, 3, 2).is_err());
    }
}
