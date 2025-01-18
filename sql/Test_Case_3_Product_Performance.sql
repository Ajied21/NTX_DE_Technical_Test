-- run this query
WITH ProductPerformance AS (
    SELECT 
        v2ProductName,
        SUM(totalTransactionRevenue) AS total_revenue,
        SUM(transactions) AS total_quantity_sold,
        SUM(CASE 
                WHEN eCommerceAction_type = 2 THEN productPrice * eCommerceAction_step
                ELSE 0 
            END) AS total_refund_amount
    FROM 
       e_commerce_session
    GROUP BY 
        v2ProductName
),
RankedProducts AS (
    SELECT 
        v2ProductName,
        total_revenue,
        total_quantity_sold,
        total_refund_amount,
        (total_revenue - total_refund_amount) AS net_revenue,
        CASE 
            WHEN total_refund_amount > 0.1 * total_revenue THEN 'Flagged' 
            ELSE 'OK' 
        END AS refund_flag
    FROM 
        ProductPerformance
)
SELECT 
    v2ProductName,
    total_revenue,
    total_quantity_sold,
    total_refund_amount,
    net_revenue,
    refund_flag
FROM 
    RankedProducts
ORDER BY 
    net_revenue DESC;