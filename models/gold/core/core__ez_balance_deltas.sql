{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'BALANCES' }} }
) }}

SELECT
    block_number,
    block_timestamp,
    eb.address AS user_address,
    NULL AS contract_address,
    prev_bal_unadj,
    prev_bal_unadj / pow(
        10,
        18
    ) AS prev_bal,
    ROUND(
        prev_bal * ep.price,
        2
    ) AS prev_bal_usd,
    current_bal_unadj,
    current_bal_unadj / pow(
        10,
        18
    ) AS current_bal,
    ROUND(
        current_bal * ep.price,
        2
    ) AS current_bal_usd,
    current_bal_unadj - prev_bal_unadj AS bal_delta_unadj,
    current_bal - prev_bal AS bal_delta,
    current_bal_usd - prev_bal_usd AS bal_delta_usd,
    'ETH' AS symbol,
    'Native Ether' AS token_name,
    18 AS decimals,
    TRUE AS has_decimal,
    ep.price IS NOT NULL AS has_price,
    COALESCE (
        eth_balance_diffs_id,
        {{ dbt_utils.generate_surrogate_key(
            ['block_number', 'address']
        ) }}
    ) AS ez_balance_deltas_id,
    GREATEST(
        eb.inserted_timestamp,
        ep.inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    GREATEST(
        eb.modified_timestamp,
        ep.modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref("silver__eth_balance_diffs") }}
    eb
    LEFT JOIN {{ ref("price__ez_hourly_token_prices") }}
    ep
    ON DATE_TRUNC(
        'hour',
        eb.block_timestamp
    ) = ep.hour
    AND ep.token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tb.address AS user_address,
    contract_address,
    prev_bal_unadj,
    CASE
        WHEN C.decimals IS NOT NULL THEN prev_bal_unadj / pow(
            10,
            C.decimals
        )
    END AS prev_bal,
    CASE
        WHEN C.decimals IS NOT NULL THEN ROUND(
            prev_bal * tp.price,
            2
        )
    END AS prev_bal_usd,
    current_bal_unadj,
    CASE
        WHEN C.decimals IS NOT NULL THEN current_bal_unadj / pow(
            10,
            C.decimals
        )
    END AS current_bal,
    CASE
        WHEN C.decimals IS NOT NULL THEN ROUND(
            current_bal * tp.price,
            2
        )
    END AS current_bal_usd,
    current_bal_unadj - prev_bal_unadj AS bal_delta_unadj,
    current_bal - prev_bal AS bal_delta,
    current_bal_usd - prev_bal_usd AS bal_delta_usd,
    C.symbol AS symbol,
    C.name AS token_name,
    C.decimals AS decimals,
    C.decimals IS NOT NULL AS has_decimal,
    tp.price IS NOT NULL AS has_price,
    COALESCE (
        token_balance_diffs_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tb.block_number', 'tb.contract_address', 'tb.address']
        ) }}
    ) AS ez_balance_deltas_id,
    GREATEST(
        COALESCE(
            tb.inserted_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            tp.inserted_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            C.inserted_timestamp,
            '2000-01-01'
        )
    ) AS inserted_timestamp,
    GREATEST(
        COALESCE(
            tb.modified_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            tp.modified_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            C.modified_timestamp,
            '2000-01-01'
        )
    ) AS modified_timestamp
FROM
    {{ ref("silver__token_balance_diffs") }}
    tb
    LEFT JOIN {{ ref("price__ez_hourly_token_prices") }}
    tp
    ON DATE_TRUNC(
        'hour',
        tb.block_timestamp
    ) = tp.hour
    AND tp.token_address = tb.contract_address
    LEFT JOIN {{ ref("silver__contracts") }} C
    ON tb.contract_address = LOWER(
        C.address
    )
