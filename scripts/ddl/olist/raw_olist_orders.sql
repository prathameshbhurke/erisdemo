CREATE TABLE IF NOT EXISTS raw_olist_orders (
    order_id                          VARCHAR(50),
    customer_id                       VARCHAR(50),
    order_status                      VARCHAR(20),
    order_purchase_timestamp          TIMESTAMP,
    order_approved_at                 TIMESTAMP,
    order_delivered_carrier_date      TIMESTAMP,
    order_delivered_customer_date     TIMESTAMP,
    order_estimated_delivery_date     TIMESTAMP
);