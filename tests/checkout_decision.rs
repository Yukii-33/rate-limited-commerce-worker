use rate_limited_commerce_worker::queue_worker::{
    decide, CommerceJob, FulfillmentAction, OrderStatus, ProductKind,
};

#[test]
fn physical_checkout_enters_fulfillment_and_keeps_receipt_amount() {
    let update = decide(CommerceJob::CheckoutPaid {
        order_id: "ord_1042".to_owned(),
        customer_id: "cus_77".to_owned(),
        email: "buyer@example.com".to_owned(),
        product: ProductKind::Physical,
        amount_cents: 12_500,
    });

    assert_eq!(update.status, OrderStatus::ReadyForFulfillment);
    assert_eq!(update.fulfillment, FulfillmentAction::PickAndPack);
    assert_eq!(update.receipt.amount_cents, 12_500);
    assert_eq!(update.receipt.recipient, "buyer@example.com");
}

#[test]
fn digital_checkout_is_fulfilled_without_pick_and_pack() {
    let update = decide(CommerceJob::CheckoutPaid {
        order_id: "ord_2048".to_owned(),
        customer_id: "cus_91".to_owned(),
        email: "reader@example.com".to_owned(),
        product: ProductKind::Digital,
        amount_cents: 2_400,
    });

    assert_eq!(update.status, OrderStatus::Fulfilled);
    assert_eq!(update.fulfillment, FulfillmentAction::GrantDownload);
}

