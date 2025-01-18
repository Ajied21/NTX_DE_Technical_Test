-- run this query
WITH UserMetrics AS (
    SELECT 
        fullVisitorId,
        AVG(timeOnSite) AS avg_timeOnSite,
        AVG(pageviews) AS avg_pageviews,
        AVG(sessionQualityDim) AS avg_sessionQualityDim
    FROM e_commerce_session
    GROUP BY fullVisitorId
),
GlobalAverages AS (
    SELECT 
        AVG(timeOnSite) AS global_avg_timeOnSite,
        AVG(pageviews) AS global_avg_pageviews
    FROM e_commerce_session
)
SELECT 
    um.fullVisitorId,
    um.avg_timeOnSite,
    um.avg_pageviews,
    um.avg_sessionQualityDim
FROM UserMetrics um
CROSS JOIN GlobalAverages ga
WHERE um.avg_timeOnSite > ga.global_avg_timeOnSite
  AND um.avg_pageviews < ga.global_avg_pageviews
ORDER BY um.avg_timeOnSite DESC;