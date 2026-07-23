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
