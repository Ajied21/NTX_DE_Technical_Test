-- run this query
WITH CountryRevenue AS (
    SELECT 
        country,
        SUM(totalTransactionRevenue) AS total_revenue
    FROM e_commerce_session
    GROUP BY country
    ORDER BY total_revenue DESC
),
ChannelRevenue AS (
    SELECT 
        channelGrouping,
        country,
        SUM(totalTransactionRevenue) AS channel_revenue
    FROM e_commerce_session
    GROUP BY channelGrouping, country
)
SELECT 
    cr.channelGrouping,
    cr.country,
    cr.channel_revenue
FROM ChannelRevenue cr
INNER JOIN CountryRevenue ctry ON cr.country = ctry.country
ORDER BY ctry.total_revenue 
DESC, cr.channel_revenue desc
LIMIT 5;