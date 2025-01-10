WITH time_gaps AS (
    SELECT 
        address,
        contract_address,
        block_timestamp,
        block_number,
        DATEDIFF('hour', 
            LAG(block_timestamp) OVER (
                PARTITION BY address, contract_address 
                ORDER BY block_timestamp
            ),
            block_timestamp
        ) as hours_between_updates,
        COUNT(*) OVER (
            PARTITION BY address, contract_address
        ) as total_updates
    FROM {{ ref('silver__token_balances') }}
    WHERE block_timestamp >= DATEADD('month', -1, CURRENT_TIMESTAMP())
),
update_patterns AS (
    SELECT 
        address,
        contract_address,
        COUNT(*) as update_count,
        AVG(CASE WHEN hours_between_updates IS NOT NULL 
            THEN hours_between_updates END) as avg_hours_between_updates,
        MAX(hours_between_updates) as max_gap,
        MIN(CASE WHEN hours_between_updates IS NOT NULL 
            THEN hours_between_updates END) as min_gap,
        -- Replace FILTER with CASE statements
        SUM(CASE WHEN hours_between_updates <= 1 THEN 1 ELSE 0 END) as hourly_updates,
        SUM(CASE WHEN hours_between_updates <= 24 THEN 1 ELSE 0 END) as daily_updates,
        MAX(total_updates) as total_updates
    FROM time_gaps
    GROUP BY 1, 2
)
SELECT 
    address,
    contract_address,
    update_count,
    avg_hours_between_updates,
    max_gap,
    min_gap,
    CASE 
        -- More sophisticated pattern detection
        WHEN (hourly_updates / NULLIF(total_updates, 0)) >= 0.8 
            AND update_count >= 168 THEN 'hourly'  -- Consistent hourly pattern
        WHEN (daily_updates / NULLIF(total_updates, 0)) >= 0.8 
            AND update_count >= 14 THEN 'daily'    -- Consistent daily pattern
        WHEN max_gap <= 168 THEN 'weekly'         -- Weekly or more frequent
        ELSE 'longer'
    END as update_frequency,
    CURRENT_TIMESTAMP() as pattern_calculated_at
FROM update_patterns