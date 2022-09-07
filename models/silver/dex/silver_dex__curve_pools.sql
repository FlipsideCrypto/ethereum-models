{{ config(
    materialized = 'incremental',
    unique_key = "pool_id"
) }}

WITH pool_tokens AS (

    SELECT
        DISTINCT LOWER(
            inputs :_pool :: STRING
        ) AS pool_add,
        (SPLIT(LOWER(value_string), '^')) AS coins
    FROM
        {{ source(
            'flipside_silver_ethereum',
            'reads'
        ) }}
    WHERE
        contract_name = 'Vyper_contract'
        AND contract_address IN (
            '0x0959158b6040d32d04c301a72cbfd6b39e21c9ae',
            LOWER('0xfD6f33A0509ec67dEFc500755322aBd9Df1bD5B8'),
            '0x90e00ace148ca3b23ac1bc8c240c2a7dd9c2d7f5',
            '0x7D86446dDb609eD0F5f8684AcF30380a356b2B4c'
        )
        AND function_name = 'get_underlying_coins'

{% if is_incremental() %}
AND block_timestamp >= CURRENT_DATE - 10
AND CURRENT_DATE < '2022-08-30'
{% endif %}
),
pool_tokens_parsed AS (
    SELECT
        pool_add,
        CASE
            WHEN VALUE :: STRING = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE VALUE :: STRING
        END AS coins,
        (ROW_NUMBER() over (PARTITION BY pool_add
    ORDER BY
        pool_add DESC) - 1) AS token_index
    FROM
        pool_tokens,
        TABLE(FLATTEN(pool_tokens.coins))
    WHERE
        VALUE :: STRING <> '0x0000000000000000000000000000000000000000'
),
backfilled_pools AS (
    SELECT
        LOWER(pool_address) :: STRING AS pool_address,
        token_index :: INTEGER AS token_index,
        LOWER(token_address) :: STRING AS token_address
    FROM
        {{ ref('silver__curve_pools_backfill') }}
),
streamline_pools AS (
    SELECT
        LOWER(contract_address) AS pool_address,
        function_signature,
        function_input AS token_id,
        CASE
            WHEN read_output :: STRING = '0x000000000000000000000000eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE CONCAT('0x', SUBSTR(read_output, 27, 40))
        END AS token_address,
        _inserted_timestamp
    FROM
        {{ ref('bronze__successful_reads') }}
    WHERE
        function_signature IN (
            '0x87cb4f57',
            '0xc6610657'
        )
        AND read_output :: STRING NOT IN (
            '0x',
            '0x0000000000000000000000000000000000000000000000000000000000000000'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
),
curve_read_type AS (
    SELECT
        pool_address,
        COUNT(
            DISTINCT function_signature
        ) AS count_f
    FROM
        streamline_pools
    GROUP BY
        1
),
curve_read_dedup AS (
    SELECT
        A.pool_address AS pool_address,
        CASE
            WHEN count_f = 1 THEN token_id
            WHEN function_signature <> '0xc6610657' THEN token_id + 1
            ELSE token_id
        END AS token_id_adj,
        token_id,
        GREATEST(
            token_id,
            token_id_adj
        ) AS token_id_final,
        CASE
            WHEN function_signature = '0xc6610657'
            AND token_id = 1
            AND count_f = 2 THEN FALSE
            ELSE TRUE
        END AS include_f,
        token_address,
        _inserted_timestamp
    FROM
        streamline_pools A
        JOIN curve_read_type b
        ON A.pool_address = b.pool_address
    WHERE
        A.pool_address NOT IN (
            SELECT
                DISTINCT pool_address
            FROM
                backfilled_pools
        )
),
combine_pools AS (
    SELECT
        pool_address,
        token_index,
        token_address,
        '2040-01-01' AS _inserted_timestamp
    FROM
        backfilled_pools
    UNION ALL
    SELECT
        pool_add AS pool_address,
        token_index,
        coins AS token_address,
        '1900-01-01' AS _inserted_timestamp
    FROM
        pool_tokens_parsed
    WHERE
        pool_add NOT IN (
            SELECT
                DISTINCT pool_address
            FROM
                curve_read_dedup
        )
    UNION ALL
    SELECT
        pool_address,
        token_id_final AS token_index,
        token_address,
        _inserted_timestamp
    FROM
        curve_read_dedup
    WHERE
        include_f
),
all_pools AS (
    SELECT
        pool_address,
        token_index,
        token_address,
        _inserted_timestamp
    FROM
        combine_pools qualify(ROW_NUMBER() over(PARTITION BY pool_address, token_address
    ORDER BY
        _inserted_timestamp DESC)) = 1
),
pools_final AS (
    SELECT
        pool_address :: STRING AS pool_address,
        token_address AS token_address,
        token_index :: INTEGER AS token_index,
        symbol :: STRING AS token_symbol,
        decimals :: INTEGER AS token_decimals,
        NAME :: STRING AS token_name,
        _inserted_timestamp :: TIMESTAMP AS _inserted_timestamp
    FROM
        all_pools
        LEFT JOIN {{ ref('core__dim_contracts') }}
        ON LOWER(token_address) = LOWER(address)
),
pool_symbols AS (
    SELECT
        pool_address,
        MIN(
            CASE
                WHEN token_index = 0 THEN token_symbol
            END
        ) AS symbol0,
        MIN(
            CASE
                WHEN token_index = 1 THEN token_symbol
            END
        ) AS symbol1,
        MIN(
            CASE
                WHEN token_index = 2 THEN token_symbol
            END
        ) AS symbol2,
        MIN(
            CASE
                WHEN token_index = 3 THEN token_symbol
            END
        ) AS symbol3,
        MIN(
            CASE
                WHEN token_index = 4 THEN token_symbol
            END
        ) AS symbol4,
        MIN(
            CASE
                WHEN token_index = 5 THEN token_symbol
            END
        ) AS symbol5,
        MIN(
            CASE
                WHEN token_index = 6 THEN token_symbol
            END
        ) AS symbol6,
        MIN(
            CASE
                WHEN token_index = 7 THEN token_symbol
            END
        ) AS symbol7
    FROM
        pools_final
    GROUP BY
        pool_address
),
pool_names AS (
    SELECT
        pool_address,
        CASE
            WHEN symbol7 IS NOT NULL THEN CONCAT(COALESCE(symbol0, ''), '-', COALESCE(symbol1, ''), '-', COALESCE(symbol2, ''), '-', COALESCE(symbol3, ''), '-', COALESCE(symbol4, ''), '-', COALESCE(symbol5, ''), '-', COALESCE(symbol6, ''), '-', COALESCE(symbol7, ''), ' curve LP')
            WHEN symbol6 IS NOT NULL THEN CONCAT(COALESCE(symbol0, ''), '-', COALESCE(symbol1, ''), '-', COALESCE(symbol2, ''), '-', COALESCE(symbol3, ''), '-', COALESCE(symbol4, ''), '-', COALESCE(symbol5, ''), '-', COALESCE(symbol6, ''), ' curve LP')
            WHEN symbol5 IS NOT NULL THEN CONCAT(COALESCE(symbol0, ''), '-', COALESCE(symbol1, ''), '-', COALESCE(symbol2, ''), '-', COALESCE(symbol3, ''), '-', COALESCE(symbol4, ''), '-', COALESCE(symbol5, ''), ' curve LP')
            WHEN symbol4 IS NOT NULL THEN CONCAT(COALESCE(symbol0, ''), '-', COALESCE(symbol1, ''), '-', COALESCE(symbol2, ''), '-', COALESCE(symbol3, ''), '-', COALESCE(symbol4, ''), ' curve LP')
            WHEN symbol3 IS NOT NULL THEN CONCAT(COALESCE(symbol0, ''), '-', COALESCE(symbol1, ''), '-', COALESCE(symbol2, ''), '-', COALESCE(symbol3, ''), ' curve LP')
            WHEN symbol2 IS NOT NULL THEN CONCAT(COALESCE(symbol0, ''), '-', COALESCE(symbol1, ''), '-', COALESCE(symbol2, ''), ' curve LP')
            ELSE CONCAT(COALESCE(symbol0, ''), '-', COALESCE(symbol1, ''), ' curve LP')
        END AS pool_name
    FROM
        pool_symbols
)
SELECT
    A.pool_address AS pool_address,
    token_address,
    token_index,
    token_symbol,
    token_decimals,
    token_name,
    pool_name,
    CONCAT(
        A.pool_address,
        token_address
    ) AS pool_id,
    _inserted_timestamp
FROM
    pools_final A
    LEFT JOIN pool_names b
    ON A.pool_address = b.pool_address
