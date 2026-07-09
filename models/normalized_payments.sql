with all_payments as (

    select * from {{ ref('stg_payments') }}

),

cash_payments as (

    select
        *
    from all_payments

    where payment_method in ('credit_card', 'bank_transfer')

    group by order_id
),

non_cash_payments as (

    select
        *,
        amount * 0.8 as normalized_value
    from all_payments

    where payment_method in ('coupon', 'gift_card')

    group by order_id
),

normalized_all_payments as (

    select * from cash_payments

    union all

    select * from non_cash_payments
)

select * from normalized_all_payments
