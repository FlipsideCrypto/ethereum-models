{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime','reorg','curation']
) }}
WITH withdraw AS(

    SELECT
        block_number,
        block_timestamp,
        event_index,
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
            WHEN contract_address = LOWER('0xC13e21B648A5Ee794902342038FF3aDAB66BE987') THEN 'Spark'
            ELSE 'ERROR'
        END AS spark_version,
        origin_to_address AS lending_pool_contract,
        CASE
            WHEN reserve_1 = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE reserve_1
        END AS spark_market
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
AND contract_address = LOWER('0xC13e21B648A5Ee794902342038FF3aDAB66BE987')
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
        {{ ref('silver__spark_tokens') }}
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
        {{ ref('silver__spark_token_prices') }}
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
    LOWER(
        spark_market
    ) AS spark_market,
    LOWER(
        atoken_meta.atoken_address
    ) AS spark_token,
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
    spark_version as platform,
    hourly_price AS token_price,
    atoken_meta.underlying_symbol AS symbol,
    'ethereum' AS blockchain,
    _log_id,
    _inserted_timestamp
FROM
    withdraw
    LEFT JOIN atoken_meta
    ON withdraw.spark_market = atoken_meta.underlying_address
    AND atoken_version = spark_version
    LEFT JOIN atoken_prices
    ON DATE_TRUNC(
        'hour',
        block_timestamp
    ) = prices_hour
    AND withdraw.spark_market = atoken_prices.underlying_address qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
