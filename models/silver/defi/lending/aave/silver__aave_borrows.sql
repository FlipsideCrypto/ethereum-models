{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH --borrows from Aave LendingPool contracts
borrow AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CASE
            WHEN topics [0] :: STRING = '0xc6a898309e823ee50bac64e45ca8adba6690e99e7841c45d754e2a38e9019d9b' THEN CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40))
            WHEN topics [0] :: STRING = '0x1e77446728e5558aa1b7e81e0cdab9cc1b075ba893b740600c76a315c2caa553' THEN CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40))
            WHEN topics [0] :: STRING = '0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0' THEN CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40))
        END AS reserve_1,
        CASE
            WHEN topics [0] :: STRING = '0xc6a898309e823ee50bac64e45ca8adba6690e99e7841c45d754e2a38e9019d9b' THEN CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))
            WHEN topics [0] :: STRING = '0x1e77446728e5558aa1b7e81e0cdab9cc1b075ba893b740600c76a315c2caa553' THEN CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))
            WHEN topics [0] :: STRING = '0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0' THEN CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))
        END AS onBehalfOf,
        CASE
            WHEN topics [0] :: STRING = '0xc6a898309e823ee50bac64e45ca8adba6690e99e7841c45d754e2a38e9019d9b' THEN utils.udf_hex_to_int(
                topics [3] :: STRING
            ) :: INTEGER
            WHEN topics [0] :: STRING = '0x1e77446728e5558aa1b7e81e0cdab9cc1b075ba893b740600c76a315c2caa553' THEN utils.udf_hex_to_int(
                topics [3] :: STRING
            ) :: INTEGER
            WHEN topics [0] :: STRING = '0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0' THEN utils.udf_hex_to_int(
                topics [3] :: STRING
            ) :: INTEGER
        END AS refferal,
        CASE
            WHEN topics [0] :: STRING = '0xc6a898309e823ee50bac64e45ca8adba6690e99e7841c45d754e2a38e9019d9b' THEN CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40))
            WHEN topics [0] :: STRING = '0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0' THEN CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40))
        END AS userAddress,
        CASE
            WHEN topics [0] :: STRING = '0xc6a898309e823ee50bac64e45ca8adba6690e99e7841c45d754e2a38e9019d9b' THEN utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            ) :: INTEGER
            WHEN topics [0] :: STRING = '0x1e77446728e5558aa1b7e81e0cdab9cc1b075ba893b740600c76a315c2caa553' THEN utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            ) :: INTEGER
            WHEN topics [0] :: STRING = '0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0' THEN utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            ) :: INTEGER
        END AS borrow_quantity,
        CASE
            WHEN topics [0] :: STRING = '0xc6a898309e823ee50bac64e45ca8adba6690e99e7841c45d754e2a38e9019d9b' THEN utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            ) :: INTEGER
            WHEN topics [1] :: STRING = '0x1e77446728e5558aa1b7e81e0cdab9cc1b075ba893b740600c76a315c2caa553' THEN utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            ) :: INTEGER
            WHEN topics [1] :: STRING = '0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0' THEN utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            ) :: INTEGER
        END AS borrow_rate_mode,
        CASE
            WHEN topics [0] :: STRING = '0xc6a898309e823ee50bac64e45ca8adba6690e99e7841c45d754e2a38e9019d9b' THEN utils.udf_hex_to_int(
                segmented_data [3] :: STRING
            ) :: INTEGER
            WHEN topics [0] :: STRING = '0x1e77446728e5558aa1b7e81e0cdab9cc1b075ba893b740600c76a315c2caa553' THEN utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            ) :: INTEGER
            WHEN topics [0] :: STRING = '0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0' THEN utils.udf_hex_to_int(
                segmented_data [3] :: STRING
            ) :: INTEGER
        END AS borrowrate,
        _inserted_timestamp,
        _log_id,
        CASE
            WHEN contract_address = LOWER('0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9') THEN 'Aave V2'
            WHEN contract_address = LOWER('0x398eC7346DcD622eDc5ae82352F02bE94C62d119') THEN 'Aave V1'
            WHEN contract_address = LOWER('0x7937d4799803fbbe595ed57278bc4ca21f3bffcb') THEN 'Aave AMM'
            WHEN contract_address = LOWER('0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2') THEN 'Aave V3'
            ELSE 'ERROR'
        END AS aave_version,
        origin_from_address AS borrower_address,
        COALESCE(
            origin_to_address,
            contract_address
        ) AS lending_pool_contract,
        CASE
            WHEN reserve_1 = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE reserve_1
        END AS aave_market
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING IN (
            '0xc6a898309e823ee50bac64e45ca8adba6690e99e7841c45d754e2a38e9019d9b',
            '0x1e77446728e5558aa1b7e81e0cdab9cc1b075ba893b740600c76a315c2caa553',
            '0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
AND contract_address IN(
    --Aave V2 LendingPool contract address
    LOWER('0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9'),
    --V2
    LOWER('0x398eC7346DcD622eDc5ae82352F02bE94C62d119'),
    --V1
    LOWER('0x7937d4799803fbbe595ed57278bc4ca21f3bffcb'),
    --AMM
    LOWER('0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2') --v3
)
AND tx_status = 'SUCCESS' --excludes failed txs
),
atoken_meta AS (
    SELECT
        atoken_address,
        atoken_symbol,
        atoken_name,
        atoken_decimals,
        underlying_address,
        underlying_symbol,
        underlying_name,
        underlying_decimals,
        atoken_version,
        atoken_created_block,
        atoken_stable_debt_address,
        atoken_variable_debt_address
    FROM
        {{ ref('silver__aave_tokens') }}
),
contracts AS (
    SELECT
        address,
        symbol,
        NAME,
        decimals,
    FROM
        {{ ref('silver__contracts') }}
    WHERE
        contract_address IN (
            SELECT
                DISTINCT aave_market
            FROM
                borrow
        )
),
atoken_prices AS (
    SELECT
        HOUR AS prices_hour,
        token_address AS underlying_address,
        symbol,
        NAME,
        decimals,
        price AS hourly_price,
        blockchain,
        is_native,
        is_imputed,
        is_deprecated,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('price__ez_prices_hourly') }}
    WHERE
        HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                borrow
        )
        AND (
            token_address IN (
                SELECT
                    aave_market
                FROM
                    borrow
            )
            OR token_address IN (
                SELECT
                    DISTINCT address
                FROM
                    contracts
            )
        )
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        borrow_quantity AS borrowed_tokens_unadj,
        LOWER(
            aave_market
        ) AS aave_market,
        LOWER(
            atoken_meta.atoken_address
        ) AS aave_token,
        borrow_quantity / pow(
            10,
            atoken_meta.underlying_decimals
        ) AS borrowed_tokens,
        borrow_quantity * hourly_price / pow(
            10,
            atoken_meta.underlying_decimals
        ) AS borrowed_usd,
        LOWER(
            borrower_address
        ) AS borrower_address,
        CASE
            WHEN borrow_rate_mode = 2 THEN 'Variable Rate'
            ELSE 'Stable Rate'
        END AS borrow_rate_mode,
        LOWER(
            lending_pool_contract
        ) AS lending_pool_contract,
        aave_version,
        hourly_price AS token_price,
        atoken_meta.underlying_symbol AS symbol,
        'ethereum' AS blockchain,
        _log_id,
        _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'event_index']
        ) }} AS aave_borrows_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        borrow
        LEFT JOIN atoken_meta
        ON borrow.aave_market = atoken_meta.underlying_address
        AND atoken_version = aave_version
        LEFT JOIN atoken_prices
        ON DATE_TRUNC(
            'hour',
            block_timestamp
        ) = prices_hour
        AND borrow.aave_market = atoken_prices.underlying_address qualify(ROW_NUMBER() over(PARTITION BY _log_id
    ORDER BY
        _inserted_timestamp DESC)) = 1
