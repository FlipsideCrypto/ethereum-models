{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH withdraw AS(

    SELECT
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS reserve_1,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS useraddress,
        CASE
            WHEN topics [0] :: STRING = '0x3115d1449a7b732c986cba18244e897a450f61e1bb8d589cd2e69e6c8924f9f7' THEN CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40))
            ELSE origin_from_address
        END AS depositor,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS withdraw_amount,
        _inserted_timestamp,
        _log_id,
        tx_hash,
        CASE
            WHEN contract_address = LOWER('0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9') THEN 'Aave V2'
            WHEN contract_address = LOWER('0x398eC7346DcD622eDc5ae82352F02bE94C62d119') THEN 'Aave V1'
            WHEN contract_address = LOWER('0x7937d4799803fbbe595ed57278bc4ca21f3bffcb') THEN 'Aave AMM'
            WHEN contract_address = LOWER('0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2') THEN 'Aave V3'
            ELSE 'ERROR'
        END AS aave_version,
        origin_to_address AS lending_pool_contract,
        CASE
            WHEN reserve_1 = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE reserve_1
        END AS aave_market
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING IN (
            '0x3115d1449a7b732c986cba18244e897a450f61e1bb8d589cd2e69e6c8924f9f7',
            '0x9c4ed599cd8555b9c1e8cd7643240d7d71eb76b792948c49fcb4d411f7b6b3c6'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '36 hours'
    FROM
        {{ this }}
)
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
atoken_prices AS (
    SELECT
        prices_hour,
        underlying_address,
        atoken_address,
        atoken_version,
        eth_price,
        oracle_price,
        backup_price,
        underlying_decimals,
        underlying_symbol,
        value_ethereum,
        hourly_price
    FROM
        {{ ref('silver__aave_token_prices') }}
    WHERE
        prices_hour :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                withdraw
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
    LOWER(
        aave_market
    ) AS aave_market,
    LOWER(
        atoken_meta.atoken_address
    ) AS aave_token,
    withdraw_amount / pow(
        10,
        atoken_meta.underlying_decimals
    ) AS withdrawn_tokens,
    withdraw_amount * hourly_price / pow(
        10,
        atoken_meta.underlying_decimals
    ) AS withdrawn_usd,
    LOWER(
        depositor
    ) AS depositor_address,
    aave_version,
    hourly_price AS token_price,
    atoken_meta.underlying_symbol AS symbol,
    'ethereum' AS blockchain,
    _log_id,
    _inserted_timestamp
FROM
    withdraw
    LEFT JOIN atoken_meta
    ON withdraw.aave_market = atoken_meta.underlying_address
    AND atoken_version = aave_version
    LEFT JOIN atoken_prices
    ON DATE_TRUNC(
        'hour',
        block_timestamp
    ) = prices_hour
    AND withdraw.aave_market = atoken_prices.underlying_address qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
