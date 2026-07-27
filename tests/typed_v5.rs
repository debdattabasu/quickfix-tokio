//! Tests for the generated FIX 5.0 (application) and FIXT.1.1 (session)
//! typed APIs. Only compiled when both features are enabled.

#![cfg(all(feature = "fix50", feature = "fixt11"))]

use quickfix_tokio::{Amount, Message, UtcTimestamp};

/// Decimal-feature-agnostic `Amount` from a string.
fn amt(s: &str) -> Amount {
    use quickfix_tokio::value::FixDecode;
    Amount::decode(0, s.as_bytes()).unwrap()
}

#[test]
fn fixt11_logon_roundtrip() {
    use quickfix_tokio::fixt11::{self, AnyMessage, fields, messages::logon::Logon};

    assert_eq!(fixt11::BEGIN_STRING, "FIXT.1.1");

    // FIXT Logon requires EncryptMethod, HeartBtInt, and DefaultApplVerID(1137).
    let mut logon = Logon::new(fields::EncryptMethod::NONE_OTHER, 30, "9"); // 9 = FIX50SP2 applver
    logon.set_username("trader-1");

    assert_eq!(logon.heart_bt_int().unwrap(), 30);
    assert_eq!(logon.default_appl_ver_id().unwrap(), "9");
    assert_eq!(logon.username().unwrap(), "trader-1");

    let raw = Message::from(logon).to_bytes();
    let parsed = Message::parse(&raw, false).unwrap();
    match fixt11::classify(parsed) {
        AnyMessage::Logon(l) => {
            assert_eq!(l.heart_bt_int().unwrap(), 30);
            assert_eq!(l.default_appl_ver_id().unwrap(), "9");
        }
        other => panic!("classified as {other:?}"),
    }
}

#[test]
fn fix50_new_order_single_roundtrip() {
    use quickfix_tokio::fix50::{self, AnyMessage, fields, messages::new_order_single::NewOrderSingle};

    assert_eq!(fix50::BEGIN_STRING, "FIX.5.0");

    let mut order = NewOrderSingle::new(
        "ORDER-1",
        fields::Side::BUY,
        UtcTimestamp::now(),
        fields::OrdType::LIMIT,
    );
    order.set_symbol("TSLA");
    order.set_order_qty(amt("100"));
    order.set_price(amt("101.25"));

    assert_eq!(order.cl_ord_id().unwrap(), "ORDER-1");
    assert_eq!(order.side().unwrap(), '1');
    assert_eq!(order.order_qty().unwrap(), amt("100"));

    let raw = Message::from(order).to_bytes();
    let parsed = Message::parse(&raw, false).unwrap();
    match fix50::classify(parsed) {
        AnyMessage::NewOrderSingle(o) => {
            assert_eq!(o.symbol().unwrap(), "TSLA");
            assert_eq!(o.price().unwrap(), amt("101.25"));
            assert_eq!(o.ord_type().unwrap(), fields::OrdType::LIMIT);
        }
        other => panic!("classified as {other:?}"),
    }
}

#[test]
fn fix50_repeating_groups() {
    use quickfix_tokio::fix50::{fields, messages::new_order_single::{NewOrderSingle, NoPartyIDs}};

    let mut order = NewOrderSingle::new(
        "ORDER-2",
        fields::Side::SELL,
        UtcTimestamp::now(),
        fields::OrdType::MARKET,
    );
    let mut p = NoPartyIDs::new();
    p.set_party_id("TRADER-A"); // delimiter first
    p.set_party_role(11);
    order.set_no_party_ids([p]);

    let raw = Message::from(order).to_bytes();
    let parsed = Message::parse(&raw, false).unwrap();
    let order = NewOrderSingle::from_message(parsed).unwrap();
    let parties = order.no_party_ids().unwrap();
    assert_eq!(parties.len(), 1);
    assert_eq!(parties[0].party_id().unwrap(), "TRADER-A");
}
