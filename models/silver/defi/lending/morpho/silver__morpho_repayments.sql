{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH 
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
        {{ ref('silver__morpho_tokens') }}
),
repay AS(

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
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS borrower_address,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS repayer,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS repayed_amount,
        _log_id,
        _inserted_timestamp,
        'Morpho' AS morpho_version,
        origin_to_address AS lending_pool_contract,
        origin_from_address AS repayer_address,
        CASE
            WHEN reserve_1 = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE reserve_1
        END AS morpho_market
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING IN (
            '0x4cdde6e09bb755c9a5589ebaec640bbfedff1362d4b255ebf8339782b9942faa',
            '0xb718f0b14f03d8c3adf35b15e3da52421b042ac879e5a689011a8b1e0036773d',
            '0xa534c8dbe71f871f9f3530e97a74601fea17b426cae02e1c5aee42c96c784051'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
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
        morpho_market
    ) AS morpho_market,
    LOWER(
        atoken_meta.atoken_address
    ) AS morpho_token,
    repayed_amount AS amount_unadj,
    repayed_amount / pow(
        10,
        atoken_meta.underlying_decimals
    ) AS amount,
    repayer_address AS payer,
    borrower_address AS borrower,
    LOWER(
        lending_pool_contract
    ) AS lending_pool_contract,
    morpho_version as platform,
    atoken_meta.underlying_symbol AS symbol,
    'ethereum' AS blockchain,
    _log_id,
    _inserted_timestamp
FROM
    repay
    LEFT JOIN atoken_meta
    ON repay.morpho_market = atoken_meta.underlying_address qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
