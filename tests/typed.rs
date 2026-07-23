//! Tests for the generated typed FIX 4.4 API.

#![cfg(feature = "fix44")]

use quickfix_tokio::fix44::{self, AnyMessage, fields, messages};
use quickfix_tokio::{Message, UtcTimestamp};

#[test]
fn typed_new_order_single_roundtrip() {
    let mut order = messages::new_order_single::NewOrderSingle::new(
        "ORDER-1",
        fields::Side::BUY,
        UtcTimestamp::now(),
        fields::OrdType::MARKET,
    );
    order.set_symbol("TSLA");
    order.set_order_qty(100.0);

    assert_eq!(order.cl_ord_id().unwrap(), "ORDER-1");
    assert_eq!(order.side().unwrap(), '1');
    assert_eq!(order.symbol().unwrap(), "TSLA");
    assert_eq!(order.order_qty().unwrap(), 100.0);
    assert!(order.has_symbol());
    assert!(!order.has_account());

    // Serialize through the generic layer and back.
    let msg: Message = order.into();
    let raw = msg.to_bytes();
    let parsed = Message::parse(&raw, false).unwrap();
    match fix44::classify(parsed) {
        AnyMessage::NewOrderSingle(o) => {
            assert_eq!(o.cl_ord_id().unwrap(), "ORDER-1");
            assert_eq!(o.ord_type().unwrap(), fields::OrdType::MARKET);
        }
        other => panic!("classified as {other:?}"),
    }
}

#[test]
fn typed_repeating_groups() {
    use messages::new_order_single::{NewOrderSingle, NoPartyIDs};

    let mut order = NewOrderSingle::new(
        "ORDER-2",
        fields::Side::SELL,
        UtcTimestamp::now(),
        fields::OrdType::LIMIT,
    );
    let mut p1 = NoPartyIDs::new();
    p1.set_party_id("TRADER-A"); // delimiter (448) first
    p1.set_party_id_source(fields::PartyIDSource::PROPCODE);
    p1.set_party_role(11);
    let mut p2 = NoPartyIDs::new();
    p2.set_party_id("DESK-9");
    p2.set_party_id_source(fields::PartyIDSource::PROPCODE);
    order.set_no_party_ids([p1, p2]);

    // Roundtrip through wire bytes.
    let raw = Message::from(order).to_bytes();
    let parsed = Message::parse(&raw, false).unwrap();
    let order = messages::new_order_single::NewOrderSingle::from_message(parsed).unwrap();

    let parties = order.no_party_ids().unwrap();
    assert_eq!(parties.len(), 2);
    assert_eq!(parties[0].party_id().unwrap(), "TRADER-A");
    assert_eq!(parties[0].party_role().unwrap(), 11);
    assert_eq!(parties[1].party_id().unwrap(), "DESK-9");
    assert!(!parties[1].has_party_role());
}

#[test]
fn typed_execution_report() {
    let mut er = messages::execution_report::ExecutionReport::new(
        "ORD-1",
        "EXEC-1",
        fields::ExecType::TRADE,
        fields::OrdStatus::FILLED,
        fields::Side::BUY,
        0.0,   // LeavesQty
        100.0, // CumQty
        100.5, // AvgPx
    );
    er.set_cl_ord_id("ORDER-1");
    er.set_last_px(100.5);

    assert_eq!(er.exec_type().unwrap(), 'F');
    assert_eq!(er.ord_status().unwrap(), '2');
    assert_eq!(er.avg_px().unwrap(), 100.5);
    assert_eq!(
        Message::from(er).msg_type().unwrap(),
        messages::execution_report::ExecutionReport::MSG_TYPE
    );
}

#[test]
fn classify_unknown_and_admin() {
    let hb = Message::with_type("0");
    assert!(matches!(fix44::classify(hb), AnyMessage::Heartbeat(_)));
    let junk = Message::with_type("ZZ");
    assert!(matches!(fix44::classify(junk), AnyMessage::Unknown(_)));
}

// ----- deeper coverage, mirroring QuickFIX/n's GenMessageTest.cs -----

/// The constructor sets MsgType in the header (TcrMsgTypeGetsSetTest).
#[test]
fn constructor_sets_msg_type() {
    use messages::new_order_single::NewOrderSingle;
    let order =
        NewOrderSingle::new("X", fields::Side::BUY, UtcTimestamp::now(), fields::OrdType::MARKET);
    assert_eq!(order.msg_type().unwrap(), NewOrderSingle::MSG_TYPE);
    assert_eq!(order.msg_type().unwrap(), "D");
}

/// A single field round-trips through set/get (TcrFieldGetterTest).
#[test]
fn field_getter_setter() {
    use messages::execution_report::ExecutionReport;
    let mut er = ExecutionReport::new(
        "O", "E", fields::ExecType::TRADE, fields::OrdStatus::FILLED, fields::Side::BUY, 0.0, 0.0,
        0.0,
    );
    er.set_avg_px(10.5);
    assert_eq!(er.avg_px().unwrap(), 10.5);
}

/// has_x() reflects presence, before and after setting (TcrIsSetTest).
#[test]
fn is_set_semantics() {
    use messages::new_order_single::NewOrderSingle;
    let mut order =
        NewOrderSingle::new("X", fields::Side::BUY, UtcTimestamp::now(), fields::OrdType::LIMIT);
    // Required fields set by the constructor are present.
    assert!(order.has_cl_ord_id());
    assert!(order.has_side());
    // An optional field is absent until set.
    assert!(!order.has_price());
    assert!(order.price().is_err());
    order.set_price(99.5);
    assert!(order.has_price());
    assert_eq!(order.price().unwrap(), 99.5);
}

/// Every field type generated for FIX 4.4 round-trips through the wire with
/// its Rust type (covers i64/f64/char/String/UtcTimestamp).
#[test]
fn field_types_roundtrip() {
    use chrono::SubsecRound;
    use messages::new_order_single::NewOrderSingle;
    // Default wire precision is milliseconds, so use a millis-truncated
    // timestamp for an exact round-trip comparison.
    let ts = UtcTimestamp::new(
        chrono::Utc::now().trunc_subsecs(3),
        quickfix_tokio::TimestampPrecision::Millis,
    );
    let mut order = NewOrderSingle::new("ORD-T", fields::Side::SELL, ts, fields::OrdType::LIMIT);
    order.set_order_qty(42.5); // f64 (QTY)
    order.set_price(101.0); // f64 (PRICE)
    order.set_max_floor(10.0); // f64
    order.set_symbol("MSFT"); // String
    order.set_account("ACC-9"); // String

    let raw = Message::from(order).to_bytes();
    let order = NewOrderSingle::from_message(Message::parse(&raw, false).unwrap()).unwrap();

    assert_eq!(order.cl_ord_id().unwrap(), "ORD-T"); // String
    assert_eq!(order.side().unwrap(), '2'); // char
    assert_eq!(order.ord_type().unwrap(), '2'); // char
    assert_eq!(order.order_qty().unwrap(), 42.5); // f64
    assert_eq!(order.price().unwrap(), 101.0);
    assert_eq!(order.symbol().unwrap(), "MSFT");
    assert_eq!(order.account().unwrap(), "ACC-9");
    assert_eq!(order.transact_time().unwrap().time, ts.time); // UtcTimestamp
}

/// A group struct exposes has_x()/get/set like a message (TcrGroupFieldGetterSetterTest).
#[test]
fn group_field_getter_setter() {
    use messages::new_order_single::NoPartyIDs;
    let mut party = NoPartyIDs::new();
    assert!(!party.has_party_id());
    assert!(!party.has_party_role());
    party.set_party_id("fooey");
    party.set_party_role(11);
    assert!(party.has_party_id());
    assert_eq!(party.party_id().unwrap(), "fooey");
    assert_eq!(party.party_role().unwrap(), 11);
}

/// A group nested inside another group round-trips (TcrGroupInGroupCtorTest):
/// NewOrderSingle -> NoAllocs -> NoAllocsNoNestedPartyIDs.
#[test]
fn nested_group_in_group_roundtrip() {
    use messages::new_order_single::{NewOrderSingle, NoAllocs, NoAllocsNoNestedPartyIDs};

    let mut order =
        NewOrderSingle::new("ORD-G", fields::Side::BUY, UtcTimestamp::now(), fields::OrdType::LIMIT);

    let mut alloc = NoAllocs::new();
    alloc.set_alloc_account("DESK-1"); // delimiter (79) first
    let mut np1 = NoAllocsNoNestedPartyIDs::new();
    np1.set_nested_party_id("NP-A"); // nested delimiter (524) first
    np1.set_nested_party_id_source('D');
    let mut np2 = NoAllocsNoNestedPartyIDs::new();
    np2.set_nested_party_id("NP-B");
    alloc.set_no_allocs_no_nested_party_ids([np1, np2]);
    order.set_no_allocs([alloc]);

    // Round-trip through wire bytes.
    let raw = Message::from(order).to_bytes();
    let order = NewOrderSingle::from_message(Message::parse(&raw, false).unwrap()).unwrap();

    let allocs = order.no_allocs().unwrap();
    assert_eq!(allocs.len(), 1);
    assert_eq!(allocs[0].alloc_account().unwrap(), "DESK-1");
    let nested = allocs[0].no_allocs_no_nested_party_ids().unwrap();
    assert_eq!(nested.len(), 2);
    assert_eq!(nested[0].nested_party_id().unwrap(), "NP-A");
    assert_eq!(nested[0].nested_party_id_source().unwrap(), 'D');
    assert_eq!(nested[1].nested_party_id().unwrap(), "NP-B");
}

/// Enum value constants carry their spec values (fields::*).
#[test]
fn enum_constants() {
    assert_eq!(fields::Side::BUY, '1');
    assert_eq!(fields::Side::SELL, '2');
    assert_eq!(fields::OrdType::MARKET, '1');
    assert_eq!(fields::OrdType::LIMIT, '2');
    assert_eq!(fields::ExecType::TRADE, 'F');
    assert_eq!(fields::OrdStatus::FILLED, '2');
    assert_eq!(fields::EncryptMethod::NONEOTHER, 0i64); // i64 enum
}
