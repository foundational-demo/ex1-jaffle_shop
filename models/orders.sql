{% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}

with orders as (

    select * from {{ ref('stg_orders') }}

),

with payments as (

    select * from {{ ref('stg_payments') }}

),

customer as (

    select * from {{ ref('stg_customers') }}

)

with order_payments as (

    select
        order_id,

        {% for payment_method in payment_methods -%}
        sum(case when payment_method = '{{ payment_method }}' then amount else 0 end) as {{ payment_method }}_amount,
        {% endfor -%}

        sum(amount) as total_amount

    from payments

    group by order_id

),

with final as (

    select
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        orders.status,
        customers.default_shipping_address as shipping_address,

        {% for payment_method in payment_methods -%}
        order_payments.{{ payment_method }}_amount,
        {% endfor -%}

        order_payments.total_amount as amount

    from orders


    left join order_payments
        on orders.order_id = order_payments.order_id

    left join customers
        on orders.customer_id = customers.customer_id
)

select * from final
