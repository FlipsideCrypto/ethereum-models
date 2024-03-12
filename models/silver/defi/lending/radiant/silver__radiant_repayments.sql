{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH repay AS(

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
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS radiant_market,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS borrower_address,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS repayer,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS repayed_amount,
        origin_to_address AS lending_pool_contract,
        origin_from_address AS repayer_address,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0x4cdde6e09bb755c9a5589ebaec640bbfedff1362d4b255ebf8339782b9942faa'

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
AND contract_address IN (
    LOWER('0x2032b9A8e9F7e76768CA9271003d3e43E1616B1F'),
    LOWER('0xF4B1486DD74D07706052A33d31d7c0AAFD0659E1')
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
        {{ ref('silver__radiant_tokens') }}
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
    radiant_market,
    atoken_meta.atoken_address AS radiant_token,
    repayed_amount AS amount_unadj,
    repayed_amount / pow(
        10,
        atoken_meta.underlying_decimals
    ) AS amount,
    repayer_address AS payer,
    borrower_address AS borrower,
    lending_pool_contract,
    CASE
        WHEN contract_address = LOWER('0x2032b9A8e9F7e76768CA9271003d3e43E1616B1F') THEN 'Radiant V1'
        WHEN contract_address = LOWER('0xF4B1486DD74D07706052A33d31d7c0AAFD0659E1') THEN 'Radiant V2'
    END AS platform,
    atoken_meta.underlying_symbol AS symbol,
    'arbitrum' AS blockchain,
    _log_id,
    _inserted_timestamp
FROM
    repay
    LEFT JOIN atoken_meta
    ON repay.radiant_market = atoken_meta.underlying_address qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
