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

WITH --borrows from Aave LendingPool contracts
borrow AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
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
            WHEN contract_address = LOWER('0xC13e21B648A5Ee794902342038FF3aDAB66BE987') THEN 'Spark'
            ELSE 'ERROR'
        END AS spark_version,
        origin_from_address AS borrower_address,
        COALESCE(
            origin_to_address,
            contract_address
        ) AS lending_pool_contract,
        CASE
            WHEN reserve_1 = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE reserve_1
        END AS spark_market
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
        MAX(
            _inserted_timestamp
        ) - INTERVAL '36 hours'
    FROM
        {{ this }}
)
{% endif %}
AND contract_address = lower('0xC13e21B648A5Ee794902342038FF3aDAB66BE987')
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
                borrow
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
    spark_version AS platform,
    hourly_price AS token_price,
    atoken_meta.underlying_symbol AS symbol,
    'ethereum' AS blockchain,
    _log_id,
    _inserted_timestamp
FROM
    borrow
    LEFT JOIN atoken_meta
    ON borrow.spark_market = atoken_meta.underlying_address
    AND atoken_version = spark_version
    LEFT JOIN atoken_prices
    ON DATE_TRUNC(
        'hour',
        block_timestamp
    ) = prices_hour
    AND borrow.spark_market = atoken_prices.underlying_address qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
