{{ config(
    materialized = 'table',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    tags = ['curated']
) }}

WITH incremental_sample AS (
    SELECT 
        block_number,
        block_timestamp,
        address,
        contract_address
    FROM ethereum.silver.token_balances
    WHERE _inserted_timestamp >= DATEADD('hour', -1, CURRENT_TIMESTAMP())  -- Simulate typical incremental load
),
previous_changes AS (
    SELECT 
        i.address,
        i.contract_address,
        i.block_timestamp::timestamp as current_time,
        MAX(h.block_timestamp::timestamp) as prev_change_time  -- Find most recent change before this one
    FROM incremental_sample i 
    LEFT JOIN ethereum.silver.token_balances h
        ON i.address = h.address
        AND i.contract_address = h.contract_address
        AND h.block_timestamp < i.block_timestamp
    GROUP BY 1,2,3
),
time_diff_buckets AS (
    SELECT 
        CASE 
            WHEN prev_change_time IS NULL THEN 'First Update'
            WHEN DATEDIFF('HOUR', prev_change_time, current_time) <= 1 THEN '≤ 1 hour'
            WHEN DATEDIFF('HOUR', prev_change_time, current_time) <= 24 THEN '≤ 24 hours'
            WHEN DATEDIFF('DAY', prev_change_time, current_time) <= 7 THEN '≤ 1 week'
            WHEN DATEDIFF('DAY', prev_change_time, current_time) <= 30 THEN '≤ 1 month'
            WHEN DATEDIFF('MONTH', prev_change_time, current_time) <= 6 THEN '≤ 6 months'
            WHEN DATEDIFF('MONTH', prev_change_time, current_time) <= 12 THEN '≤ 1 year'
            ELSE '> 1 year'
        END as time_difference_bucket,
        COUNT(*) as number_of_records,
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as percentage
    FROM previous_changes
    GROUP BY 1
    ORDER BY 
        CASE time_difference_bucket
            WHEN 'First Update' THEN 0
            WHEN '≤ 1 hour' THEN 1
            WHEN '≤ 24 hours' THEN 2
            WHEN '≤ 1 week' THEN 3
            WHEN '≤ 1 month' THEN 4
            WHEN '≤ 6 months' THEN 5
            WHEN '≤ 1 year' THEN 6
            ELSE 7
        END
)
SELECT 
    time_difference_bucket,
    number_of_records,
    ROUND(percentage, 2) as percentage
FROM time_diff_buckets