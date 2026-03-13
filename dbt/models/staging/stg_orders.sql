with source as (
    select * from raw_orders
),

cleaned as (
    select
        order_id,
        customer_id,
        date(order_date)                    as order_date,
        lower(trim(status))                 as status,
        round(amount, 2)                    as order_amount,
        case
            when status = 'completed' then true
            else false
        end                                 as is_completed
    from source
    where order_id is not null
)

select * from cleaned
