use lfs_protocol::*;
fn packet() -> Packet {
    Packet {
        identity: Identity {
            dataset: "d".into(),
            device: "a".into(),
            incarnation: "i".into(),
        },
        schema: 1,
        seq: 1,
        predecessor: None,
        operations: vec![Operation {
            id: OpId {
                dataset: "d".into(),
                device: "a".into(),
                incarnation: "i".into(),
                seq: 1,
                index: 0,
                schema: 1,
            },
            table: 1,
            key: "k".into(),
            parents: vec![],
            row: Some(vec![
                Value::Text("k".into()),
                Value::Null,
                Value::Integer(-2),
                Value::Real(1.5),
                Value::Blob(vec![0, 255]),
            ]),
        }],
    }
}
#[test]
fn fixed_wire_vector_and_type_preservation() {
    let bytes = packet().encode().unwrap();
    let hex: String = bytes.iter().map(|v| format!("{v:02x}")).collect();
    assert_eq!(
        hex,
        concat!(
            "4c46533100000001",
            "0000000164",
            "0000000161",
            "0000000169",
            "00000001",
            "0000000000000001",
            "00",
            "00000001",
            "00000001",
            "03000000016b",
            "00000000",
            "01",
            "00000005",
            "03000000016b",
            "00",
            "01fffffffffffffffe",
            "023ff8000000000000",
            "040000000200ff"
        )
    );
    assert_eq!(bytes, packet().encode().unwrap());
    assert_eq!(
        hash_hex(&hash(b"abc")),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
}
#[test]
fn invalid_packet_contracts_fail_closed() {
    let mut p = packet();
    p.operations[0]
        .row
        .as_mut()
        .unwrap()
        .push(Value::Real(f64::INFINITY));
    assert!(p.encode().is_err());
    let mut p = packet();
    p.operations[0].row.as_mut().unwrap()[0] = Value::Null;
    assert!(p.encode().is_err());
    let mut p = packet();
    p.operations[0].id.seq = 2;
    assert!(p.encode().is_err());
    let mut p = packet();
    let mut duplicate = p.operations[0].clone();
    duplicate.id.index = 1;
    p.operations.push(duplicate);
    assert!(p.encode().is_err());
    let mut p = packet();
    p.operations[0]
        .row
        .as_mut()
        .unwrap()
        .push(Value::Blob(vec![0; MAX_CELL + 1]));
    assert!(p.encode().is_err());
    let mut p = packet();
    p.operations[0].row = Some(
        std::iter::once(Value::Text("k".into()))
            .chain((0..17).map(|_| Value::Blob(vec![0; MAX_CELL])))
            .collect(),
    );
    assert!(p.encode().is_err());
}
