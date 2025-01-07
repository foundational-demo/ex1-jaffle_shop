with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ ref('raw_orders') }}

),

renamed as (

    select
        id as order_id,
        user_id as customer_id,
        order_date,
        status

    from source

),

order_with_total_amount as (
    select 
        r.*,
        o.status as status,
        (
            o.credit_card_amount + 
            o.coupon_amount +
            o.bank_transfer_amount +
            o.gift_card_amount
        ) as total_amount
    from renamed r
    join orders o on o.order_id = r.order_id
)

    
select * from order_with_total_amount
