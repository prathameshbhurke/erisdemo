CREATE TABLE IF NOT EXISTS raw_olist_order_payments (
    order_id                VARCHAR(50),
    payment_sequential      INTEGER,
    payment_type            VARCHAR(20),
    payment_installments    INTEGER,
    payment_value           DECIMAL(10,2)
);
