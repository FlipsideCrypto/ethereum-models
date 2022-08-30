{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    cluster_by = ['_inserted_timestamp::date']
) }}

WITH market_reads AS (

    SELECT
        contract_address,
        block_number,
        function_signature,
        read_output,
        regexp_substr_all(SUBSTR(read_output, 3, len(read_output)), '.{64}') AS segmented_data,
        _inserted_timestamp
    FROM
        {{ ref('bronze__comp_market_stats') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) :: DATE - 2
        FROM
            {{ this }}
    )
{% endif %}
),
comptroller_reads AS (
    SELECT
        contract_address,
        block_number,
        function_signature,
        read_output,
        regexp_substr_all(SUBSTR(read_output, 3, len(read_output)), '.{64}') AS segmented_data
    FROM
        {{ ref('bronze__comp_comptroller_stats') }}
    WHERE
        read_output :: STRING <> '0x'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
spine AS (
    SELECT
        block_number,
        contract_address,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM
        market_reads
    GROUP BY
        1,
        2
),
total_supply AS (
    SELECT
        block_number,
        contract_address,
        PUBLIC.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) AS total_supply
    FROM
        market_reads
    WHERE
        function_signature = '0x18160ddd'
),
borrow_rate_per_block AS (
    SELECT
        block_number,
        contract_address,
        PUBLIC.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) AS borrow_rate_per_block
    FROM
        market_reads
    WHERE
        function_signature = '0xf8f9da28'
),
exchange_rate_stored AS (
    SELECT
        block_number,
        contract_address,
        PUBLIC.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) AS exchange_rate_stored
    FROM
        market_reads
    WHERE
        function_signature = '0x182df0f5'
),
supply_rate_per_block AS (
    SELECT
        block_number,
        contract_address,
        PUBLIC.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) AS supply_rate_per_block
    FROM
        market_reads
    WHERE
        function_signature = '0xae9d70b0'
),
total_borrows AS (
    SELECT
        block_number,
        contract_address,
        PUBLIC.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) AS total_borrows
    FROM
        market_reads
    WHERE
        function_signature = '0x47bd3718'
),
total_reserves AS (
    SELECT
        block_number,
        contract_address,
        PUBLIC.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) AS total_reserves
    FROM
        market_reads
    WHERE
        function_signature = '0x8f840ddd'
),
comp_supply_speeds AS (
    SELECT
        block_number,
        contract_address,
        PUBLIC.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) AS comp_supply_speeds
    FROM
        comptroller_reads
    WHERE
        function_signature = '0x6aa875b5'
),
comp_borrow_speeds AS (
    SELECT
        block_number,
        contract_address,
        PUBLIC.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) AS comp_borrow_speeds
    FROM
        comptroller_reads
    WHERE
        function_signature = '0xf4a433c0'
),
comp_speeds AS (
    SELECT
        block_number,
        contract_address,
        PUBLIC.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) AS comp_speeds
    FROM
        comptroller_reads
    WHERE
        function_signature = '0x1d7b33d7'
),
token_meta AS (
    SELECT
        ctoken_address,
        ctoken_symbol,
        ctoken_name,
        ctoken_decimals,
        underlying_asset_address,
        underlying_decimals,
        underlying_symbol
    FROM
        {{ ref('compound__ez_asset_details') }}
),
token_prices AS (
    SELECT
        HOUR,
        token_address,
        AVG(price) AS price
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        token_address IN (
            SELECT
                DISTINCT underlying_asset_address
            FROM
                token_meta
        )
    GROUP BY
        1,
        2
),
blocks AS (
    SELECT
        block_number,
        block_timestamp
    FROM
        {{ ref('silver__blocks') }}
)
SELECT
    s.block_number,
    b.block_timestamp,
    DATE_TRUNC(
        'hour',
        b.block_timestamp
    ) AS block_hour,
    s.contract_address,
    s._inserted_timestamp,
    total_supply,
    total_supply / pow(
        10,
        underlying_decimals
    ) AS supply_token_amount,
    borrow_rate_per_block,
    pow(
        borrow_rate_per_block / 1e18 * ((60 / 13.15) * 60 * 24) + 1,
        365
    ) - 1 AS borrow_apy,
    exchange_rate_stored,
    exchange_rate_stored / pow(
        10,
        18 + (
            underlying_decimals - ctoken_decimals
        )
    ) * token_prices.price AS ctoken_price,
    supply_rate_per_block,
    pow(
        supply_rate_per_block / 1e18 * ((60 / 13.15) * 60 * 24) + 1,
        365
    ) - 1 AS supply_apy,
    total_borrows,
    total_borrows / pow(
        10,
        underlying_decimals
    ) AS borrows_token_amount,
    total_reserves,
    total_reserves / pow(
        10,
        underlying_decimals
    ) AS reserves_token_amount,
    comp_supply_speeds / 1e18 * comp_p.price AS comp_supply_usd,
    comp_borrow_speeds / 1e18 * comp_p.price AS comp_borrow_usd,
    comp_speeds / 1e18 * comp_p.price AS comp_speed_usd,
    CONCAT(
        s.block_number,
        '-',
        s.contract_address
    ) AS id,
    supply_token_amount * ctoken_price AS supply_usd,
    reserves_token_amount * token_prices.price AS reserves_usd,
    borrows_token_amount * token_prices.price AS borrows_usd,
    CASE
        WHEN borrows_usd != 0 THEN power(
            (
                1 + (
                    (
                        COALESCE(NULLIF(comp_speed_usd, 0), comp_borrow_usd) * 24
                    ) / borrows_usd
                )
            ),
            365
        ) -1
        ELSE NULL
    END AS comp_apy_borrow,
    CASE
        WHEN supply_usd != 0 THEN power(
            (
                1 + (
                    (
                        COALESCE(NULLIF(comp_speed_usd, 0), comp_supply_usd) * 24
                    ) / supply_usd
                )
            ),
            365
        ) -1
        ELSE NULL
    END AS comp_apy_supply
FROM
    spine s
    LEFT JOIN blocks b
    ON s.block_number = b.block_number
    LEFT JOIN total_supply
    ON total_supply.block_number = s.block_number
    AND total_supply.contract_address = s.contract_address
    LEFT JOIN borrow_rate_per_block
    ON borrow_rate_per_block.block_number = s.block_number
    AND borrow_rate_per_block.contract_address = s.contract_address
    LEFT JOIN exchange_rate_stored
    ON exchange_rate_stored.block_number = s.block_number
    AND exchange_rate_stored.contract_address = s.contract_address
    LEFT JOIN supply_rate_per_block
    ON supply_rate_per_block.block_number = s.block_number
    AND supply_rate_per_block.contract_address = s.contract_address
    LEFT JOIN total_borrows
    ON total_borrows.block_number = s.block_number
    AND total_borrows.contract_address = s.contract_address
    LEFT JOIN total_reserves
    ON total_reserves.block_number = s.block_number
    AND total_reserves.contract_address = s.contract_address
    LEFT JOIN comp_supply_speeds
    ON comp_supply_speeds.block_number = s.block_number
    AND comp_supply_speeds.contract_address = s.contract_address
    LEFT JOIN comp_borrow_speeds
    ON comp_borrow_speeds.block_number = s.block_number
    AND comp_borrow_speeds.contract_address = s.contract_address
    LEFT JOIN comp_speeds
    ON comp_speeds.block_number = s.block_number
    AND comp_speeds.contract_address = s.contract_address
    LEFT JOIN token_meta
    ON s.contract_address = token_meta.ctoken_address
    LEFT JOIN token_prices
    ON DATE_TRUNC(
        'hour',
        b.block_timestamp
    ) = token_prices.hour
    AND token_prices.token_address = underlying_asset_address
    LEFT JOIN token_prices comp_p
    ON DATE_TRUNC(
        'hour',
        b.block_timestamp
    ) = comp_p.hour
    AND comp_p.token_address = '0xc00e94cb662c3520282e6f5717214004a7f26888'
