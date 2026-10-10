//! Mandated I05-I10 public API acceptance. MemoryTransport is MODEL/FAULT only.
use lfs_protocol::{MAX_PACKET, OpId, Operation, Packet, Register, hash};
use lfs_sqlite::*;
use std::{
    collections::BTreeSet,
    future::Future,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};
static SERIAL: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "lfs-s2-{}-{}",
            std::process::id(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&p).unwrap();
        Self(p)
    }
    fn store(&self, d: &str) -> Store {
        Store::open(
            self.0.join(format!("{d}.db")),
            identity(d),
            schema(),
            Baseline::Reject,
        )
        .unwrap()
    }
    fn sdk(&self, d: &str) -> Sdk {
        Sdk::open(
            self.0.join(format!("{d}.db")),
            identity(d),
            schema(),
            Baseline::Reject,
        )
        .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn identity(d: &str) -> Identity {
    Identity {
        dataset: "data".into(),
        device: d.into(),
        incarnation: "inc1".into(),
    }
}
fn schema() -> Schema {
    Schema {
        version: 1,
        tables: vec![Table {
            id: 1,
            name: "items".into(),
            columns: vec![
                Column {
                    name: "id".into(),
                    kind: ColumnType::Text,
                    nullable: false,
                },
                Column {
                    name: "title".into(),
                    kind: ColumnType::Text,
                    nullable: false,
                },
                Column {
                    name: "n".into(),
                    kind: ColumnType::Integer,
                    nullable: false,
                },
            ],
        }],
    }
}
fn row(k: &str, title: &str, n: i64) -> Vec<Value> {
    vec![
        Value::Text(k.into()),
        Value::Text(title.into()),
        Value::Integer(n),
    ]
}
fn add(s: &mut Store, k: &str) -> Commit {
    s.write_transaction(|w| {
        w.execute(
            "INSERT INTO items VALUES (?1,'base',0)",
            &[Value::Text(k.into())],
        )
    })
    .unwrap()
    .1
    .unwrap()
}
fn update(s: &mut Store, title: &str, n: i64) -> Commit {
    s.write_transaction(|w| {
        w.execute(
            "UPDATE items SET title=?1,n=?2 WHERE id='k'",
            &[Value::Text(title.into()), Value::Integer(n)],
        )
    })
    .unwrap()
    .1
    .unwrap()
}
fn package(c: &Commit) -> Package {
    Package::from_pending(c.pending.clone(), &schema())
}
fn rows(s: &mut Store) -> Vec<Vec<Value>> {
    s.read(|r| r.query("SELECT * FROM items ORDER BY id", &[]))
        .unwrap()
}
fn send(s: &mut Store, c: &Commit) -> InboxState {
    s.receive(package(c)).unwrap()
}
fn parents(s: &mut Store) -> Vec<OpId> {
    s.heads(1, "k").unwrap().into_iter().map(|o| o.id).collect()
}
struct WakeThread(std::thread::Thread);
impl Wake for WakeThread {
    fn wake(self: Arc<Self>) {
        self.0.unpark()
    }
}
fn run<F: Future>(f: F) -> F::Output {
    let w = Waker::from(Arc::new(WakeThread(std::thread::current())));
    let mut cx = Context::from_waker(&w);
    let mut f = std::pin::pin!(f);
    loop {
        match f.as_mut().poll(&mut cx) {
            Poll::Ready(v) => return v,
            Poll::Pending => std::thread::park(),
        }
    }
}
fn sync(s: &Sdk, t: &impl Transport) -> SyncReport {
    run(s.sync_once(t, &Cancellation::default(), SyncLimits::default())).unwrap()
}
fn crafted(
    d: &str,
    seq: u64,
    prev: Option<[u8; 32]>,
    key: &str,
    parents: Vec<OpId>,
    value: Option<Vec<Value>>,
) -> Package {
    let id = identity(d);
    let mut parents: Vec<_> = parents.iter().map(OpId::encode).collect();
    parents.sort();
    let p = Packet {
        identity: id.clone(),
        schema: 1,
        seq,
        predecessor: prev,
        operations: vec![Operation {
            id: OpId {
                dataset: id.dataset.clone(),
                device: id.device.clone(),
                incarnation: id.incarnation.clone(),
                seq,
                index: 0,
                schema: 1,
            },
            table: 1,
            key: key.into(),
            parents,
            row: value,
        }],
    };
    let bytes = p.encode().unwrap();
    Package {
        key: PacketKey {
            identity: id,
            seq,
            hash: hash(&bytes),
        },
        bytes,
        fingerprint: schema().fingerprint(),
        attachment_references: vec![],
    }
}
#[test]
fn offline_three_devices_full_row_conflicts_and_atomic_ordinary_write_rejection() {
    let f = Fixture::new();
    let (mut a, mut b, mut c) = (f.store("a"), f.store("b"), f.store("c"));
    let base = add(&mut a, "k");
    send(&mut b, &base);
    send(&mut c, &base);
    let aa = update(&mut a, "a-title", 0);
    let bb = update(&mut b, "base", 7);
    let cc = update(&mut c, "c-title", 8);
    for s in [&mut a, &mut b, &mut c] {
        for p in [&aa, &bb, &cc] {
            send(s, p);
        }
        assert_eq!(s.heads(1, "k").unwrap().len(), 3);
        assert_eq!(rows(s), vec![row("k", "c-title", 8)]);
        assert_eq!(s.list_conflicts().unwrap().len(), 1);
        let before = s.pending_packets(100).unwrap();
        assert!(matches!(
            s.write_transaction(|w| {
                w.execute("INSERT INTO items VALUES ('unrelated','x',1)", &[])?;
                w.execute("UPDATE items SET title='forbidden' WHERE id='k'", &[])
            }),
            Err(Error::ConflictRequiresResolution { .. })
        ));
        assert_eq!(rows(s), vec![row("k", "c-title", 8)]);
        assert_eq!(s.pending_packets(100).unwrap(), before);
    }
    assert_eq!(a.heads(1, "k").unwrap(), b.heads(1, "k").unwrap());
    assert_eq!(b.heads(1, "k").unwrap(), c.heads(1, "k").unwrap());
}
#[test]
fn delete_edit_tombstone_heads_and_explicit_delete_resolution() {
    let f = Fixture::new();
    let (mut a, mut z) = (f.store("a"), f.store("z"));
    let base = add(&mut a, "k");
    send(&mut z, &base);
    let edit = update(&mut a, "edit", 1);
    let del = z
        .write_transaction(|w| w.execute("DELETE FROM items WHERE id='k'", &[]))
        .unwrap()
        .1
        .unwrap();
    send(&mut a, &del);
    send(&mut z, &edit);
    assert!(rows(&mut a).is_empty());
    assert_eq!(a.heads(1, "k").unwrap().len(), 2);
    assert!(a.heads(1, "k").unwrap().iter().any(|o| o.row.is_none()));
    let observed = parents(&mut a);
    let resolution = a.resolve(1, "k", observed, None).unwrap();
    send(&mut z, &resolution);
    assert_eq!(a.heads(1, "k").unwrap(), z.heads(1, "k").unwrap());
    assert_eq!(a.heads(1, "k").unwrap().len(), 1);
    assert!(rows(&mut z).is_empty());
}
#[test]
fn stale_resolve_unseen_descendant_and_simultaneous_resolutions_survive() {
    let f = Fixture::new();
    let (mut a, mut b, mut c, mut d) = (f.store("a"), f.store("b"), f.store("c"), f.store("d"));
    let base = add(&mut a, "k");
    for s in [&mut b, &mut c, &mut d] {
        send(s, &base);
    }
    let aa = update(&mut a, "a", 1);
    let bb = update(&mut b, "b", 2);
    for s in [&mut a, &mut b, &mut c, &mut d] {
        send(s, &aa);
        send(s, &bb);
    }
    let observed = parents(&mut a);
    let branch = d
        .resolve(
            1,
            "k",
            vec![aa.packet.operations[0].id.clone()],
            Some(row("k", "unseen", 9)),
        )
        .unwrap();
    send(&mut a, &branch);
    let ar = a
        .resolve(1, "k", observed.clone(), Some(row("k", "merged-a", 3)))
        .unwrap();
    let br = b
        .resolve(1, "k", observed, Some(row("k", "merged-b", 4)))
        .unwrap();
    for s in [&mut a, &mut b, &mut c, &mut d] {
        for op in [&branch, &ar, &br] {
            send(s, op);
        }
        let hs = s.heads(1, "k").unwrap();
        assert_eq!(hs.len(), 3);
        let got: BTreeSet<_> = hs.iter().map(|o| o.id.clone()).collect();
        assert_eq!(
            got,
            BTreeSet::from([
                branch.packet.operations[0].id.clone(),
                ar.packet.operations[0].id.clone(),
                br.packet.operations[0].id.clone()
            ])
        );
        assert_eq!(rows(s), vec![row("k", "unseen", 9)]);
    }
}
#[test]
fn dependencies_restart_predecessor_unrelated_record_and_delayed_ancestors() {
    let f = Fixture::new();
    let mut a = f.store("a");
    let v0 = add(&mut a, "k");
    let v1 = update(&mut a, "v1", 1);
    let v2 = update(&mut a, "v2", 2);
    let unrelated = add(&mut a, "other");
    {
        let mut b = f.store("b");
        assert_eq!(send(&mut b, &unrelated), InboxState::WaitingDependencies);
        assert_eq!(send(&mut b, &v2), InboxState::WaitingDependencies);
        assert_eq!(send(&mut b, &v1), InboxState::WaitingDependencies);
        assert!(rows(&mut b).is_empty());
        assert!(b.applied_packets().unwrap().is_empty());
        assert!(!b.status().unwrap().healthy());
    }
    let mut b = f.store("b");
    assert_eq!(send(&mut b, &v0), InboxState::Applied);
    assert!(b.status().unwrap().healthy());
    assert_eq!(b.applied_packets().unwrap().len(), 4);
    let expected = rows(&mut b);
    assert_eq!(expected, vec![row("k", "v2", 2), row("other", "base", 0)]);
    send(&mut b, &v0);
    send(&mut b, &v1);
    assert_eq!(rows(&mut b), expected);
    assert!(b.pending_packets(10).unwrap().is_empty());
    drop(b);
    let mut b = f.store("b");
    send(&mut b, &v0);
    assert_eq!(rows(&mut b), expected);
    assert_eq!(
        b.heads(1, "k").unwrap(),
        vec![v2.packet.operations[0].clone()]
    );
}
#[test]
fn cross_device_record_parent_wait_and_wrong_record_parent_rejected() {
    let f = Fixture::new();
    let mut a = f.store("a");
    let base = add(&mut a, "k");
    let child = crafted(
        "b",
        1,
        None,
        "k",
        vec![base.packet.operations[0].id.clone()],
        Some(row("k", "child", 1)),
    );
    let mut c = f.store("c");
    assert_eq!(c.receive(child).unwrap(), InboxState::WaitingDependencies);
    assert!(c.applied_packets().unwrap().is_empty());
    send(&mut c, &base);
    assert_eq!(rows(&mut c), vec![row("k", "child", 1)]);
    let bad = crafted(
        "evil",
        1,
        None,
        "different",
        vec![base.packet.operations[0].id.clone()],
        Some(row("different", "bad", 1)),
    );
    let bytes = bad.bytes.clone();
    let key = bad.key.clone();
    assert!(c.receive(bad).is_err());
    assert_eq!(c.retained_bytes(&key).unwrap(), bytes);
    assert_eq!(c.applied_packets().unwrap().len(), 2);
    assert_eq!(rows(&mut c), vec![row("k", "child", 1)]);
    assert!(!c.status().unwrap().healthy());
}
#[test]
fn same_stream_sequence_fork_stops_visibly_and_survives_restart() {
    let f = Fixture::new();
    let mut s = f.store("receiver");
    let p = crafted("a", 1, None, "k", vec![], Some(row("k", "one", 1)));
    let q = crafted("a", 1, None, "k", vec![], Some(row("k", "fork", 2)));
    let qb = q.bytes.clone();
    let qk = q.key.clone();
    s.receive(p.clone()).unwrap();
    assert!(matches!(s.receive(q), Err(Error::StreamFork)));
    assert_eq!(s.retained_bytes(&qk).unwrap(), qb);
    assert_eq!(rows(&mut s), vec![row("k", "one", 1)]);
    drop(s);
    let mut s = f.store("receiver");
    assert_eq!(s.status().unwrap().stopped_streams, 1);
    assert!(matches!(s.receive(p), Err(Error::StreamFork)));
    assert_eq!(s.applied_packets().unwrap().len(), 1);
}
#[test]
fn invalid_packets_preserved_atomically_and_do_not_advance() {
    let f = Fixture::new();
    let good = crafted("a", 1, None, "k", vec![], Some(row("k", "good", 1)));
    for case in 0..12 {
        let mut s = f.store(&format!("r{case}"));
        let mut p = good.clone();
        match case {
            0 => p.bytes[7] = 2,
            1 => p.fingerprint.push('x'),
            2 => {
                let mut v = Packet::decode(&p.bytes).unwrap();
                v.schema = 2;
                v.operations[0].id.schema = 2;
                p.bytes = v.encode().unwrap()
            }
            3 => {
                let mut v = Packet::decode(&p.bytes).unwrap();
                v.operations[0].table = 2;
                p.bytes = v.encode().unwrap()
            }
            4 => {
                let mut v = Packet::decode(&p.bytes).unwrap();
                v.operations[0].row.as_mut().unwrap().pop();
                p.bytes = v.encode().unwrap()
            }
            5 => {
                let mut v = Packet::decode(&p.bytes).unwrap();
                v.operations[0].row.as_mut().unwrap()[1] = Value::Null;
                p.bytes = v.encode().unwrap()
            }
            6 => {
                let mut v = Packet::decode(&p.bytes).unwrap();
                v.operations[0].row.as_mut().unwrap()[2] = Value::Text("wrong".into());
                p.bytes = v.encode().unwrap()
            }
            7 => p.bytes = vec![0; MAX_PACKET + 1],
            8 => p.key.identity.dataset = "other".into(),
            9 => p.attachment_references.push("unsupported".into()),
            10 => p.bytes.push(0),
            11 => p.key.hash = [99; 32],
            _ => unreachable!(),
        }
        if case != 11 {
            p.key.hash = hash(&p.bytes)
        }
        let key = p.key.clone();
        let bytes = p.bytes.clone();
        assert!(s.receive(p).is_err(), "case {case}");
        assert!(rows(&mut s).is_empty());
        assert!(s.applied_packets().unwrap().is_empty());
        assert!(s.pending_packets(10).unwrap().is_empty());
        assert_eq!(s.retained_bytes(&key).unwrap(), bytes);
        assert!(!s.status().unwrap().healthy());
    }
}
#[test]
fn malformed_duplicates_parents_keys_and_identity_are_not_decoded() {
    let base = crafted("a", 1, None, "k", vec![], Some(row("k", "good", 1)));
    let p = Packet::decode(&base.bytes).unwrap();
    let mut v = p.clone();
    let mut dup = v.operations[0].clone();
    dup.id.index = 1;
    v.operations.push(dup);
    assert!(v.encode().is_err());
    let mut v = p.clone();
    v.operations[0].id.index = 1;
    assert!(v.encode().is_err());
    let mut v = p.clone();
    let id = OpId {
        device: "parent".into(),
        ..v.operations[0].id.clone()
    };
    v.operations[0].parents = vec![id.encode(), id.encode()];
    assert!(v.encode().is_err());
    let mut v = p.clone();
    v.operations[0].row.as_mut().unwrap()[0] = Value::Text("mismatch".into());
    assert!(v.encode().is_err());
    // Encode valid two-key packet then corrupt second key into duplicate (without encoder).
    let mut v = p;
    let mut op = v.operations[0].clone();
    op.id.index = 1;
    op.key = "z".into();
    op.row = Some(row("z", "good", 1));
    v.operations.push(op);
    let mut b = v.encode().unwrap();
    for byte in &mut b {
        if *byte == b'z' {
            *byte = b'k'
        }
    }
    assert!(Packet::decode(&b).is_err());
    for n in 0..base.bytes.len() {
        assert!(Packet::decode(&base.bytes[..n]).is_err());
    }
}
// Independent oracle: transitive reachability matrix over generated graph indices. No production
// head-removal implementation or projection comparator is used to compute expected survivor set.
fn oracle(ops: &[Operation]) -> Vec<Operation> {
    let n = ops.len();
    let mut reach = vec![vec![false; n]; n];
    for (i, o) in ops.iter().enumerate() {
        for p in &o.parents {
            let j = ops.iter().position(|o| o.id.encode() == *p).unwrap();
            reach[i][j] = true;
        }
    }
    for k in 0..n {
        for i in 0..n {
            for j in 0..n {
                reach[i][j] |= reach[i][k] && reach[k][j];
            }
        }
    }
    ops.iter()
        .enumerate()
        .filter(|(j, _)| !(0..n).any(|i| i != *j && reach[i][*j]))
        .map(|(_, o)| o.clone())
        .collect()
}
fn permutations(v: &mut [usize], i: usize, out: &mut Vec<Vec<usize>>) {
    if i == v.len() {
        out.push(v.to_vec());
        return;
    }
    for j in i..v.len() {
        v.swap(i, j);
        permutations(v, i + 1, out);
        v.swap(i, j);
    }
}
#[test]
fn exhaustive_valid_set_permutations_repeats_and_independent_reachability_oracle() {
    let f = Fixture::new();
    let p0 = crafted("a", 1, None, "k", vec![], Some(row("k", "v0", 0)));
    let o0 = Packet::decode(&p0.bytes).unwrap().operations.remove(0);
    let p1 = crafted(
        "b",
        1,
        None,
        "k",
        vec![o0.id.clone()],
        Some(row("k", "v1", 1)),
    );
    let o1 = Packet::decode(&p1.bytes).unwrap().operations.remove(0);
    let p2 = crafted("b", 2, Some(p1.key.hash), "k", vec![o1.id.clone()], None);
    let p3 = crafted(
        "c",
        1,
        None,
        "k",
        vec![o0.id.clone()],
        Some(row("k", "w", 3)),
    );
    let packets = [p0, p1, p2, p3];
    let ops: Vec<_> = packets
        .iter()
        .map(|p| Packet::decode(&p.bytes).unwrap().operations.remove(0))
        .collect();
    let expected = oracle(&ops);
    assert_eq!(expected, vec![ops[2].clone(), ops[3].clone()]);
    let mut orders = Vec::new();
    permutations(&mut [0, 1, 2, 3], 0, &mut orders);
    for (i, order) in orders.iter().enumerate() {
        let mut s = f.store(&format!("r{i}"));
        let mut reg = Register::default();
        let mut waiting = Vec::new();
        for &j in order {
            for _ in 0..2 {
                s.receive(packets[j].clone()).unwrap();
                waiting.push(ops[j].clone());
                loop {
                    let before = waiting.len();
                    waiting.retain(|o| reg.insert(o.clone()).is_err());
                    if before == waiting.len() {
                        break;
                    }
                }
            }
        }
        assert_eq!(s.heads(1, "k").unwrap(), expected);
        assert_eq!(reg.heads(), expected);
        assert_eq!(reg.projection(), Some(ops[3].clone()));
        assert_eq!(rows(&mut s), vec![row("k", "w", 3)]);
        assert_eq!(
            s.applied_packets()
                .unwrap()
                .into_iter()
                .collect::<BTreeSet<_>>(),
            packets.iter().map(|p| p.key.clone()).collect()
        );
        assert!(s.pending_packets(10).unwrap().is_empty());
        assert!(s.status().unwrap().healthy());
    }
}
fn rng(seed: &mut u64) -> u64 {
    *seed = seed
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *seed
}
#[test]
fn randomized_bounded_causal_graphs_converge_with_repeats_and_restart() {
    let f = Fixture::new();
    let mut seed = 0x5eed_u64;
    for case in 0..12 {
        let mut packets: Vec<Package> = Vec::new();
        let mut ops: Vec<Operation> = Vec::new();
        for i in 0..8 {
            let mut ps = Vec::new();
            for o in &ops {
                if rng(&mut seed).is_multiple_of(3) {
                    ps.push(o.id.clone());
                }
            }
            let v = if rng(&mut seed).is_multiple_of(4) {
                None
            } else {
                Some(row("k", &format!("v{i}"), i))
            };
            let p = crafted(&format!("node{i}"), 1, None, "k", ps, v);
            ops.push(Packet::decode(&p.bytes).unwrap().operations.remove(0));
            packets.push(p);
        }
        let expected = oracle(&ops);
        let winner = expected.last().unwrap().clone(); // Generated devices node0..node7 have known ascending parsed identities.
        for perm in 0..6 {
            let name = format!("case{case}p{perm}");
            let mut order: Vec<_> = (0..8).collect();
            for i in (1..8).rev() {
                let j = rng(&mut seed) as usize % (i + 1);
                order.swap(i, j);
            }
            let mut s = f.store(&name);
            for (step, i) in order.into_iter().enumerate() {
                s.receive(packets[i].clone()).unwrap();
                s.receive(packets[i].clone()).unwrap();
                if step == 3 {
                    drop(s);
                    s = f.store(&name);
                }
            }
            assert_eq!(s.heads(1, "k").unwrap(), expected);
            assert_eq!(
                rows(&mut s),
                winner.row.clone().into_iter().collect::<Vec<_>>()
            );
            assert_eq!(s.applied_packets().unwrap().len(), 8);
            assert!(s.status().unwrap().healthy());
        }
    }
}
#[test]
fn memory_sync_readback_pagination_duplicate_missing_late_and_lost_ack_reopen() {
    let f = Fixture::new();
    let t = MemoryTransport::default();
    let a = f.sdk("a");
    for k in ["a", "b", "c"] {
        a.write_transaction(|w| {
            w.execute(
                "INSERT INTO items VALUES (?1,'base',0)",
                &[Value::Text(k.into())],
            )
        })
        .unwrap();
    }
    t.inject(Fault::LostAck).unwrap();
    assert!(matches!(
        run(a.sync_once(&t, &Cancellation::default(), SyncLimits::default())),
        Err(Error::Network(NetworkError::LostAck))
    ));
    let uploaded = t.objects()[0].clone();
    assert_eq!(a.status().unwrap().pending_outbox, 3);
    assert!(!a.status().unwrap().healthy());
    a.close().unwrap();
    let a = f.sdk("a");
    assert!(sync(&a, &t).healthy());
    assert_eq!(t.objects().len(), 3);
    assert_eq!(t.objects()[0], uploaded);
    for (i, fault) in [Fault::MissingPage, Fault::DuplicatePage, Fault::LatePage]
        .into_iter()
        .enumerate()
    {
        let b = f.sdk(&format!("b{i}"));
        t.inject(fault).unwrap();
        let l = SyncLimits {
            page_size: 2,
            ..Default::default()
        };
        let first = run(b.sync_once(&t, &Cancellation::default(), l)).unwrap();
        assert!(!first.healthy());
        assert!(sync(&b, &t).healthy());
        assert_eq!(
            b.read(|r| r.query("SELECT * FROM items ORDER BY id", &[]))
                .unwrap(),
            vec![
                row("a", "base", 0),
                row("b", "base", 0),
                row("c", "base", 0)
            ]
        );
        assert_eq!(b.status().unwrap().pending_outbox, 0);
    }
}
#[test]
fn network_failures_cancellation_retry_budget_and_event_bound() {
    let f = Fixture::new();
    for (i, e) in [
        NetworkError::Auth,
        NetworkError::RateLimit,
        NetworkError::Timeout,
        NetworkError::Cancelled,
        NetworkError::Transient,
    ]
    .into_iter()
    .enumerate()
    {
        let a = f.sdk(&format!("a{i}"));
        let t = MemoryTransport::default();
        a.write_transaction(|w| w.execute("INSERT INTO items VALUES ('k','base',0)", &[]))
            .unwrap();
        t.inject(Fault::Upload(e.clone())).unwrap();
        let l = SyncLimits {
            retries: 0,
            ..Default::default()
        };
        assert!(
            matches!(run(a.sync_once(&t,&Cancellation::default(),l)),Err(Error::Network(got)) if got==e)
        );
        assert_eq!(a.status().unwrap().pending_outbox, 1);
        assert!(t.objects().is_empty());
        assert!(!a.status().unwrap().healthy());
        assert!(sync(&a, &t).healthy());
    }
    let a = f.sdk("cancel");
    let t = MemoryTransport::default();
    let c = Cancellation::default();
    c.cancel();
    for _ in 0..70 {
        assert!(matches!(
            run(a.sync_once(&t, &c, SyncLimits::default())),
            Err(Error::Network(NetworkError::Cancelled))
        ))
    }
    assert_eq!(a.events().len(), 64);
    let t = MemoryTransport::default();
    t.inject(Fault::List(NetworkError::Timeout)).unwrap();
    t.inject(Fault::List(NetworkError::RateLimit)).unwrap();
    assert!(sync(&a, &t).healthy());
}
#[test]
fn corrupt_download_is_retained_and_never_acknowledged_or_applied() {
    let f = Fixture::new();
    let t = MemoryTransport::default();
    let a = f.sdk("a");
    a.write_transaction(|w| w.execute("INSERT INTO items VALUES ('k','base',0)", &[]))
        .unwrap();
    t.inject(Fault::CorruptDownload).unwrap();
    assert!(run(a.sync_once(&t, &Cancellation::default(), SyncLimits::default())).is_err());
    assert_eq!(a.status().unwrap().pending_outbox, 1);
    sync(&a, &t);
    let b = f.sdk("b");
    t.inject(Fault::CorruptDownload).unwrap();
    // R3/R4: object failures report incomplete work while other objects may progress.
    assert!(
        !run(b.sync_once(&t, &Cancellation::default(), SyncLimits::default()))
            .unwrap()
            .healthy()
    );
    assert!(
        b.read(|r| r.query("SELECT * FROM items", &[]))
            .unwrap()
            .is_empty()
    );
    assert_eq!(b.status().unwrap().inbox[0].state, InboxState::Retryable);
    assert!(!b.status().unwrap().healthy());
}
struct ProbeTransport {
    sdk: Arc<Sdk>,
    memory: MemoryTransport,
    pending: bool,
}
impl Transport for ProbeTransport {
    fn upload<'a>(
        &'a self,
        p: OutgoingEnvelope,
        c: &'a Cancellation,
    ) -> lfs_sqlite::sync::NetworkFuture<'a, ()> {
        self.memory.upload(p, c)
    }
    fn download<'a>(
        &'a self,
        k: &'a ObjectKey,
        c: &'a Cancellation,
    ) -> lfs_sqlite::sync::NetworkFuture<'a, Package> {
        self.memory.download(k, c)
    }
    fn list<'a>(
        &'a self,
        d: &'a str,
        cursor: usize,
        n: usize,
        c: &'a Cancellation,
    ) -> lfs_sqlite::sync::NetworkFuture<'a, lfs_sqlite::sync::Page> {
        Box::pin(async move {
            assert!(matches!(self.sdk.close(), Err(Error::SyncInProgress)));
            assert!(matches!(
                self.sdk
                    .sync_once(&self.memory, c, SyncLimits::default())
                    .await,
                Err(Error::SyncInProgress)
            ));
            self.sdk
                .write_transaction(|w| {
                    w.execute(
                        "INSERT INTO items VALUES ('during_network','unlocked',1)",
                        &[],
                    )
                })
                .unwrap();
            assert_eq!(
                self.sdk
                    .read(|r| r.query("SELECT count(*) FROM items", &[]))
                    .unwrap(),
                vec![vec![Value::Integer(1)]]
            );
            if self.pending {
                std::future::pending().await
            } else {
                self.memory.list(d, cursor, n, c).await
            }
        })
    }
}
#[test]
fn pending_network_is_cancellable_without_db_mutex_and_sync_is_exclusive() {
    let f = Fixture::new();
    let sdk = Arc::new(f.sdk("a"));
    let t = ProbeTransport {
        sdk: sdk.clone(),
        memory: MemoryTransport::default(),
        pending: true,
    };
    let c = Cancellation::default();
    let mut future = Box::pin(sdk.sync_once(&t, &c, SyncLimits::default()));
    let w = Waker::from(Arc::new(WakeThread(std::thread::current())));
    let mut cx = Context::from_waker(&w);
    assert!(future.as_mut().poll(&mut cx).is_pending());
    c.cancel();
    assert!(matches!(
        future.as_mut().poll(&mut cx),
        Poll::Ready(Err(Error::Network(NetworkError::Cancelled)))
    ));
    drop(future);
    assert_eq!(sdk.status().unwrap().pending_outbox, 1);
    assert!(sync(&sdk, &t.memory).healthy());
    sdk.close().unwrap();
    let sdk = f.sdk("a");
    assert_eq!(
        sdk.read(|r| r.query("SELECT * FROM items", &[])).unwrap(),
        vec![row("during_network", "unlocked", 1)]
    );
}
#[test]
fn busy_reentrant_write_and_bounded_scan_continue_next_call() {
    let f = Fixture::new();
    let a = f.sdk("a");
    a.write_transaction(|w| {
        assert!(matches!(
            a.read(|r| r.query("SELECT * FROM items", &[])),
            Err(Error::Busy)
        ));
        w.execute("INSERT INTO items VALUES ('a','base',0)", &[])
    })
    .unwrap();
    a.write_transaction(|w| w.execute("INSERT INTO items VALUES ('b','base',0)", &[]))
        .unwrap();
    let t = MemoryTransport::default();
    sync(&a, &t);
    let b = f.sdk("b");
    let limited = SyncLimits {
        pages: 1,
        page_size: 1,
        ..Default::default()
    };
    let r = run(b.sync_once(&t, &Cancellation::default(), limited)).unwrap();
    assert!(!r.complete_scan);
    assert!(!r.healthy());
    assert!(sync(&b, &t).healthy());
    assert_eq!(
        b.read(|r| r.query("SELECT * FROM items ORDER BY id", &[]))
            .unwrap(),
        vec![row("a", "base", 0), row("b", "base", 0)]
    );
}
#[test]
fn predecessor_mismatch_and_incarnation_streams_do_not_share_progress() {
    let f = Fixture::new();
    let mut s = f.store("r");
    let p = crafted("a", 1, None, "k", vec![], Some(row("k", "base", 0)));
    s.receive(p.clone()).unwrap();
    let q = crafted(
        "a",
        2,
        Some([42; 32]),
        "other",
        vec![],
        Some(row("other", "x", 0)),
    );
    assert!(matches!(s.receive(q), Err(Error::StreamFork)));
    assert_eq!(s.applied_packets().unwrap().len(), 1);
    let mut p2 = Packet::decode(&p.bytes).unwrap();
    p2.identity.incarnation = "inc2".into();
    p2.operations[0].id.incarnation = "inc2".into();
    p2.operations[0].row = Some(row("k", "inc2", 2));
    let bytes = p2.encode().unwrap();
    let p2 = Package {
        key: PacketKey {
            identity: p2.identity,
            seq: 1,
            hash: hash(&bytes),
        },
        bytes,
        fingerprint: schema().fingerprint(),
        attachment_references: vec![],
    };
    assert_eq!(s.receive(p2).unwrap(), InboxState::Applied);
    assert_eq!(s.heads(1, "k").unwrap().len(), 2);
    assert_eq!(s.applied_packets().unwrap().len(), 2);
    assert!(!s.status().unwrap().healthy());
}
#[test]
fn whole_missing_page_is_unhealthy_and_bounded_cursor_resumes_after_restart() {
    let f = Fixture::new();
    let t = MemoryTransport::default();
    for k in ["a", "b", "c"] {
        t.put(crafted(k, 1, None, k, vec![], Some(row(k, "base", 0))))
            .unwrap();
    }
    let b = f.sdk("r");
    t.inject(Fault::MissingPage).unwrap();
    let r = sync(&b, &t);
    assert!(!r.complete_scan);
    assert!(!r.healthy());
    assert!(
        b.read(|q| q.query("SELECT * FROM items", &[]))
            .unwrap()
            .is_empty()
    );
    let limits = SyncLimits {
        pages: 1,
        page_size: 1,
        downloads: 1,
        ..Default::default()
    };
    let r = run(b.sync_once(&t, &Cancellation::default(), limits.clone())).unwrap();
    assert!(!r.complete_scan);
    b.close().unwrap();
    let b = f.sdk("r");
    let r = run(b.sync_once(&t, &Cancellation::default(), limits.clone())).unwrap();
    assert!(!r.complete_scan);
    let r = run(b.sync_once(&t, &Cancellation::default(), limits)).unwrap();
    assert!(r.healthy());
    assert_eq!(
        b.read(|q| q.query("SELECT * FROM items ORDER BY id", &[]))
            .unwrap(),
        vec![
            row("a", "base", 0),
            row("b", "base", 0),
            row("c", "base", 0)
        ]
    );
}
#[test]
fn parsed_identity_projection_not_encoded_length_and_pure_fork() {
    let f = Fixture::new();
    let mut s = f.store("r");
    let aa = crafted("aa", 1, None, "k", vec![], Some(row("k", "aa", 1)));
    let b = crafted("b", 1, None, "k", vec![], None);
    s.receive(b.clone()).unwrap();
    s.receive(aa.clone()).unwrap();
    assert!(rows(&mut s).is_empty());
    let ops = s.heads(1, "k").unwrap();
    assert_eq!(ops.len(), 2);
    assert!(ops[1].row.is_none());
    assert_eq!(ops[1].id.device, "b");
    let mut register = Register::default();
    register.insert(ops[0].clone()).unwrap();
    let mut fork = ops[0].clone();
    fork.id.schema = 2;
    assert!(register.insert(fork).is_err());
}
#[test]
fn remote_list_and_download_errors_never_apply_and_oversize_is_bounded() {
    let f = Fixture::new();
    for (i, e) in [
        NetworkError::Timeout,
        NetworkError::Auth,
        NetworkError::RateLimit,
        NetworkError::Cancelled,
        NetworkError::Transient,
    ]
    .into_iter()
    .enumerate()
    {
        for download in [false, true] {
            let s = f.sdk(&format!("r{i}{download}"));
            let t = MemoryTransport::default();
            t.put(crafted("a", 1, None, "k", vec![], Some(row("k", "x", 1))))
                .unwrap();
            t.inject(if download {
                Fault::Download(e.clone())
            } else {
                Fault::List(e.clone())
            })
            .unwrap();
            let result = run(s.sync_once(
                &t,
                &Cancellation::default(),
                SyncLimits {
                    retries: 0,
                    ..Default::default()
                },
            ));
            if !download || matches!(e, NetworkError::Auth | NetworkError::Cancelled) {
                assert!(result.is_err());
            } else {
                assert!(!result.unwrap().healthy());
            }
            assert!(
                s.read(|r| r.query("SELECT * FROM items", &[]))
                    .unwrap()
                    .is_empty()
            );
            assert!(s.status().unwrap().inbox.is_empty());
            assert!(!s.status().unwrap().healthy());
        }
    }
    let mut s = f.store("bounded");
    let mut p = crafted("a", 1, None, "k", vec![], None);
    p.bytes = vec![0; 2 * MAX_PACKET + 1];
    assert!(s.receive(p).is_err());
    assert!(s.applied_packets().unwrap().is_empty());
}
#[test]
fn malformed_duplicate_parent_and_multiop_invalid_row_fail_all() {
    let f = Fixture::new();
    let base = crafted("a", 1, None, "k", vec![], Some(row("k", "base", 0)));
    let parent = Packet::decode(&base.bytes).unwrap().operations[0]
        .id
        .clone();
    let p = crafted(
        "b",
        1,
        None,
        "k",
        vec![parent.clone()],
        Some(row("k", "child", 0)),
    );
    let id = parent.encode();
    let at = p.bytes.windows(id.len()).position(|w| w == id).unwrap();
    let mut bytes = p.bytes.clone();
    bytes[at - 8..at - 4].copy_from_slice(&2u32.to_be_bytes());
    let duplicate = [(id.len() as u32).to_be_bytes().to_vec(), id].concat();
    bytes.splice(
        at + duplicate.len() - 4..at + duplicate.len() - 4,
        duplicate,
    );
    assert!(Packet::decode(&bytes).is_err());
    let mut s = f.store("r");
    let mut p = Packet::decode(&base.bytes).unwrap();
    let mut op = p.operations[0].clone();
    op.id.index = 1;
    op.key = "z".into();
    op.row = Some(vec![
        Value::Text("z".into()),
        Value::Null,
        Value::Integer(1),
    ]);
    p.operations.push(op);
    let bytes = p.encode().unwrap();
    let p = Package {
        key: PacketKey {
            identity: p.identity,
            seq: p.seq,
            hash: hash(&bytes),
        },
        bytes,
        fingerprint: schema().fingerprint(),
        attachment_references: vec![],
    };
    assert!(s.receive(p).is_err());
    assert!(rows(&mut s).is_empty());
    assert!(s.heads(1, "k").unwrap().is_empty());
    assert!(s.applied_packets().unwrap().is_empty());
}
#[test]
fn missing_page_marker_survives_bounded_scan_restart_and_status_is_not_healthy() {
    let f = Fixture::new();
    let t = MemoryTransport::default();
    for d in ["a", "b"] {
        t.put(crafted(d, 1, None, d, vec![], Some(row(d, "v", 1))))
            .unwrap();
    }
    let s = f.sdk("r");
    let limits = SyncLimits {
        pages: 1,
        page_size: 1,
        ..Default::default()
    };
    t.inject(Fault::MissingPage).unwrap();
    assert!(
        !run(s.sync_once(&t, &Cancellation::default(), limits.clone()))
            .unwrap()
            .healthy()
    );
    assert!(!s.status().unwrap().healthy());
    s.close().unwrap();
    let s = f.sdk("r");
    assert!(
        !run(s.sync_once(&t, &Cancellation::default(), limits))
            .unwrap()
            .healthy()
    );
    assert!(!s.status().unwrap().healthy());
    assert!(sync(&s, &t).healthy());
    assert_eq!(
        s.read(|r| r.query("SELECT * FROM items ORDER BY id", &[]))
            .unwrap(),
        vec![row("a", "v", 1), row("b", "v", 1)]
    );
}
#[test]
fn same_claimed_hash_corrupt_variant_is_preserved_without_overwriting_original() {
    // R1: invalid integrity is quarantined evidence, never a valid competing stream.
    let f = Fixture::new();
    let mut local = f.store("local");
    let original = add(&mut local, "k");
    let mut corrupted = package(&original);
    corrupted.bytes.clear();
    assert!(matches!(local.receive(corrupted), Err(Error::Invalid(_))));
    assert_eq!(local.status().unwrap().stopped_streams, 0);
    assert_eq!(local.failed_attempts().unwrap().len(), 1);
    local
        .write_transaction(|w| w.execute("DELETE FROM items", &[]))
        .unwrap();
    assert!(rows(&mut local).is_empty());
    let mut s = f.store("r");
    let p = crafted("a", 1, None, "k", vec![], Some(row("k", "valid", 1)));
    let mut corrupt = p.clone();
    *corrupt.bytes.last_mut().unwrap() ^= 1;
    s.receive(p.clone()).unwrap();
    assert!(matches!(s.receive(corrupt.clone()), Err(Error::Invalid(_))));
    assert_eq!(s.retained_bytes(&p.key).unwrap(), p.bytes);
    let attempts = s.failed_attempts().unwrap();
    assert_eq!(attempts.len(), 1);
    assert!(attempts[0].bytes.ends_with(&corrupt.bytes));
    assert_eq!(rows(&mut s), vec![row("k", "valid", 1)]);
    assert_eq!(s.applied_packets().unwrap().len(), 1);
    drop(s);
    let mut s = f.store("r");
    assert_eq!(s.status().unwrap().stopped_streams, 0);
    assert_eq!(s.failed_attempts().unwrap().len(), 1);
}
