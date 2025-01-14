{{ config(
    materialized = 'table',
    cluster_by = ['address', 'contract_address'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    tags = ['curated']
) }}

WITH last_updates AS (
    -- Get the last update timestamp and total update count for each address/contract pair
    SELECT 
        address,
        contract_address,
        MAX(block_timestamp) as last_update_time,
        COUNT(*) as total_updates,
        MIN(block_timestamp) as first_update_time
    FROM {{ ref('silver__token_balances') }}
    WHERE block_timestamp >= DATEADD('month', -3, CURRENT_TIMESTAMP())
    GROUP BY 1, 2
),
activity_metrics AS (
    -- Calculate activity patterns based on update history
    SELECT 
        lu.address,
        lu.contract_address,
        lu.last_update_time,
        lu.total_updates,
        DATEDIFF('hour', lu.first_update_time, lu.last_update_time) as total_hours_active,
        CASE
            WHEN lu.last_update_time >= DATEADD('day', -7, CURRENT_TIMESTAMP()) 
            AND (lu.total_updates / NULLIF(DATEDIFF('hour', lu.first_update_time, lu.last_update_time), 0)) >= 0.8 
            THEN 'hourly'
            WHEN lu.last_update_time >= DATEADD('month', -1, CURRENT_TIMESTAMP())
            AND (lu.total_updates / NULLIF(DATEDIFF('day', lu.first_update_time, lu.last_update_time), 0)) >= 0.8
            THEN 'daily'
            WHEN lu.last_update_time >= DATEADD('month', -3, CURRENT_TIMESTAMP())
            THEN 'weekly'
            ELSE 'longer'
        END as update_frequency,
        -- Additional metrics for pattern analysis
        lu.total_updates / NULLIF(DATEDIFF('hour', lu.first_update_time, lu.last_update_time), 0) as updates_per_hour,
        lu.total_updates / NULLIF(DATEDIFF('day', lu.first_update_time, lu.last_update_time), 0) as updates_per_day
    FROM last_updates lu
)
SELECT 
    address,
    contract_address,
    last_update_time,
    total_updates,
    total_hours_active,
    update_frequency,
    updates_per_hour,
    updates_per_day,
    CURRENT_TIMESTAMP() as pattern_calculated_at
FROM activity_metrics