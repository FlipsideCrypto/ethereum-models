{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}
WITH --borrows from Aave LendingPool contracts
atoken_meta AS (
    SELECT
        atoken_address,
        version_pool,
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
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS reserve_1,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS onBehalfOf,
        utils.udf_hex_to_int(
                topics [3] :: STRING
            ) :: INTEGER
        AS refferal,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS userAddress,
        utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            ) :: INTEGER
        AS borrow_quantity,
        utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            ) :: INTEGER
        AS borrow_rate_mode,
        utils.udf_hex_to_int(
                segmented_data [3] :: STRING
            ) :: INTEGER
        AS borrowrate,
        _inserted_timestamp,
        _log_id,
        'Spark' AS spark_version,
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
        topics [0] :: STRING = '0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
AND contract_address IN (SELECT distinct(version_pool) from atoken_meta)
AND tx_status = 'SUCCESS' --excludes failed txs
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
        spark_market
    ) AS spark_market,
    LOWER(
        atoken_meta.atoken_address
    ) AS spark_token,
    borrow_quantity AS amount_unadj,
    borrow_quantity / pow(
        10,
        atoken_meta.underlying_decimals
    ) AS amount,
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
    atoken_meta.underlying_symbol AS symbol,
    atoken_meta.underlying_decimals AS underlying_decimals,
    'ethereum' AS blockchain,
    _log_id,
    _inserted_timestamp
FROM
    borrow
    LEFT JOIN atoken_meta
    ON borrow.spark_market = atoken_meta.underlying_address qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
