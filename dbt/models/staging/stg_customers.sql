with source as (
    select * from raw_customers
),

cleaned as (
    select
        customer_id,
        first_name,
        last_name,
        first_name || ' ' || last_name      as full_name,
        lower(trim(email))                  as email,
        date(created_at)                    as customer_since
    from source
    where customer_id is not null
)

select * from cleaned
