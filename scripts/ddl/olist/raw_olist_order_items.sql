CREATE TABLE IF NOT EXISTS raw_olist_order_items (
    order_id                VARCHAR(50),
    order_item_id           INTEGER,
    product_id              VARCHAR(50),
    seller_id               VARCHAR(50),
    shipping_limit_date     TIMESTAMP,
    price                   DECIMAL(10,2),
    freight_value           DECIMAL(10,2)
);
