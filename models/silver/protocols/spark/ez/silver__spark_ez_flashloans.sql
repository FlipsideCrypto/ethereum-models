{{ config(
    materialized = 'incremental',
    unique_key = "_log_id",
    cluster_by = ['block_timestamp::DATE'],
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'AAVE',
                'PURPOSE': 'DEFI'
            }
        }
    },
    tags = ['non_realtime'],
    persist_docs ={ "relation": true,
    "columns": true }
) }}

WITH flashloan AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS target_address,
        CASE
            WHEN topics [0] :: STRING = '0x631042c832b07452973831137f2d73e395028b44b250dedc5abb0ee766e168ac' THEN CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))
            WHEN topics [0] :: STRING = '0xefefaba5e921573100900a3ad9cf29f222d995fb3b6045797eaea7521bd8d6f0' THEN origin_to_address
            ELSE origin_from_address
        END AS initiator_address,
        CASE
            WHEN topics [0] :: STRING = '0x631042c832b07452973831137f2d73e395028b44b250dedc5abb0ee766e168ac' THEN CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40))
            WHEN topics [0] :: STRING = '0x5b8f46461c1dd69fb968f1a003acee221ea3e19540e350233b612ddb43433b55' THEN CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))
            WHEN topics [0] :: STRING = '0xefefaba5e921573100900a3ad9cf29f222d995fb3b6045797eaea7521bd8d6f0' THEN CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))
        END AS asset_1,
        CASE
            WHEN topics [0] :: STRING = '0xefefaba5e921573100900a3ad9cf29f222d995fb3b6045797eaea7521bd8d6f0' THEN 
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER
        ELSE
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER END AS flashloan_quantity,
        CASE
            WHEN topics [0] :: STRING = '0xefefaba5e921573100900a3ad9cf29f222d995fb3b6045797eaea7521bd8d6f0' THEN 
        utils.udf_hex_to_int(
            segmented_data [4] :: STRING
        ) :: INTEGER 
        ELSE
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER END AS premium_quantity,
        CASE
            WHEN topics [0] :: STRING = '0xefefaba5e921573100900a3ad9cf29f222d995fb3b6045797eaea7521bd8d6f0' THEN 
        utils.udf_hex_to_int(
            topics[3] :: STRING
        ) :: INTEGER 
        ELSE
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) :: INTEGER END AS refferalCode,
        _log_id,
        _inserted_timestamp,
        COALESCE(
            origin_to_address,
            contract_address
        ) AS lending_pool_contract,
        CASE
            WHEN contract_address = LOWER('0xC13e21B648A5Ee794902342038FF3aDAB66BE987') THEN 'Spark'
            ELSE 'ERROR'
        END AS aave_version,
        CASE
            WHEN asset_1 = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE asset_1
        END AS aave_market
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING IN (
            '0x631042c832b07452973831137f2d73e395028b44b250dedc5abb0ee766e168ac',
            '0x5b8f46461c1dd69fb968f1a003acee221ea3e19540e350233b612ddb43433b55',
            '0xefefaba5e921573100900a3ad9cf29f222d995fb3b6045797eaea7521bd8d6f0'
        )

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
                flashloan
        )
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    LOWER(
        aave_market
    ) AS aave_market,
    LOWER(
        atoken_meta.atoken_address
    ) AS aave_token,
    flashloan_quantity / pow(
        10,
        atoken_meta.underlying_decimals
    ) AS flashloan_amount,
    flashloan_quantity * hourly_price / pow(
        10,
        atoken_meta.underlying_decimals
    ) AS flashloan_amount_usd,
    premium_quantity / pow(
        10,
        atoken_meta.underlying_decimals
    ) AS premium_amount,
    premium_quantity * hourly_price / pow(
        10,
        atoken_meta.underlying_decimals
    ) AS premium_amount_usd,
    LOWER(initiator_address) AS initiator_address,
    LOWER(target_address) AS target_address,
    aave_version AS protocol,
    hourly_price AS token_price,
    atoken_meta.underlying_symbol AS symbol,
    'ethereum' AS blockchain,
    _log_id,
    _inserted_timestamp
FROM
    flashloan
    LEFT JOIN atoken_meta
    ON flashloan.aave_market = atoken_meta.underlying_address
    AND atoken_version = aave_version
    LEFT JOIN atoken_prices
    ON DATE_TRUNC(
        'hour',
        block_timestamp
    ) = prices_hour
    AND flashloan.aave_market = atoken_prices.underlying_address qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
