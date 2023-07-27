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
        segmented_data,
        _inserted_timestamp
    FROM
        {{ ref('silver__reads') }}
    WHERE
        function_signature IN (
            '0x18160ddd',
            '0xf8f9da28',
            '0x182df0f5',
            '0xae9d70b0',
            '0x47bd3718',
            '0x8f840ddd'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 1
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
        segmented_data
    FROM
        {{ ref('silver__reads') }}
    WHERE
        function_signature IN (
            '0x1d7b33d7',
            '0x6aa875b5',
            '0xf4a433c0'
        )
        AND read_output :: STRING <> '0x'
        AND contract_address = '0x3d9819210a31b4961b30ef54be2aed79b9c9cd3b'

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
        utils.udf_hex_to_int(
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
        utils.udf_hex_to_int(
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
        utils.udf_hex_to_int(
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
        utils.udf_hex_to_int(
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
        utils.udf_hex_to_int(
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
        utils.udf_hex_to_int(
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
        utils.udf_hex_to_int(
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
        utils.udf_hex_to_int(
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
        utils.udf_hex_to_int(
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
    s.contract_address AS ctoken_address,
    s._inserted_timestamp,
    total_supply,
    total_supply / pow(
        10,
        ctoken_decimals
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
    NULLIF(
        comp_supply_speeds / 1e18 * comp_p.price,
        0
    ) AS comp_supply_usd,
    NULLIF(
        comp_borrow_speeds / 1e18 * comp_p.price,
        0
    ) AS comp_borrow_usd,
    NULLIF(
        comp_speeds / 1e18 * comp_p.price,
        0
    ) AS comp_speed_usd,
    CONCAT(
        s.block_number,
        '-',
        s.contract_address
    ) AS id,
    supply_token_amount * ctoken_price AS supply_usd,
    reserves_token_amount * token_prices.price AS reserves_usd,
    borrows_token_amount * token_prices.price AS borrows_usd,
    NULLIF(
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
        END,
        0
    ) AS comp_apy_borrow,
    NULLIF(
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
        END,
        0
    ) AS comp_apy_supply,
    underlying_asset_address AS underlying_contract,
    underlying_symbol,
    token_prices.price AS token_price,
    comp_p.price AS comp_price,
    ctoken_symbol AS contract_name,
    comp_speeds AS comp_speed
FROM
    spine s
    JOIN blocks b
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
    INNER JOIN token_meta
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
    AND comp_p.token_address = '0xc00e94cb662c3520282e6f5717214004a7f26888' qualify(ROW_NUMBER() over(PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
