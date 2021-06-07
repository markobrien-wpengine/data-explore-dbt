with customer as (
    SELECT *
        FROM {{ ref('stg_customer') }}
),
orders as (
    SELECT *
        FROM {{ ref('stg_orders') }}
),
state_map as (
    SELECT *
        FROM {{ ref('stg_states') }}
),
final as (
    SELECT
        customer.customer_unique_id,
        orders.order_id,
        case
            orders.order_status
            when 'delivered' then 1
            else 0
        end
            as order_status,
        state_map.state_name
    FROM orders
    INNER JOIN customer
        ON orders.customer_order_id = customer.customer_order_id
    INNER JOIN state_map
        ON customer.customer_st = state_map.st
 )

SELECT *
    FROM final
