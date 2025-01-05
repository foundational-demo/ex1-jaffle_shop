{% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}

with orders as (

    select * from {{ ref('stg_orders') }}

),

with payments as (

    select * from {{ ref('stg_payments') }}

),

with order_payments as (

    select
        order_id,

        {% for payment_method in payment_methods -%}
        sum(case when payment_method = '{{ payment_method }}' then amount else 0 end) as {{ payment_method }}_amount,
        {% endfor -%}

        sum(case
         when payment_method = 'credit_card' then amount
         when payment_method = 'coupon' then amount
         when payment_method = 'bank_transfer' then amount
         when payment_method = 'bitcoin' then amount
         when payment_method = 'check' then amount
         when payment_method = 'gold' then amount
        end) as total_amount

    from payments

    group by order_id

),

with final as (

    select
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        orders.status,

        {% for payment_method in payment_methods -%}

        order_payments.{{ payment_method }}_amount,

        {% endfor -%}

        order_payments.total_amount as amount

    from orders


    left join order_payments
        on orders.order_id = order_payments.order_id

)

select * from final
