{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH liquidation AS(

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
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS collateral_asset,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS debt_asset,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS borrower_address,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS debt_to_cover_amount,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS liquidated_amount,
        CONCAT('0x', SUBSTR(segmented_data [2] :: STRING, 25, 40)) AS liquidator_address,
        COALESCE(
            origin_to_address,
            contract_address
        ) AS lending_pool_contract,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0xe76026d190f8c969db64638eaf9bc7087a3758e7fe58c017135a5051b4d7c4f8'

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
    collateral_asset,
    amc.atoken_address AS collateral_radiant_token,
    liquidated_amount AS amount_unadj,
    liquidated_amount / pow(
        10,
        amc.atoken_decimals
    ) AS amount,
    debt_asset,
    amd.atoken_address AS debt_radiant_token,
    liquidator_address AS liquidator,
    borrower_address AS borrower,
    CASE
        WHEN contract_address = LOWER('0x2032b9A8e9F7e76768CA9271003d3e43E1616B1F') THEN 'Radiant V1'
        WHEN contract_address = LOWER('0xF4B1486DD74D07706052A33d31d7c0AAFD0659E1') THEN 'Radiant V2'
    END AS platform,
    amc.underlying_symbol AS collateral_token_symbol,
    amd.underlying_symbol AS debt_token_symbol,
    'arbitrum' AS blockchain,
    _log_id,
    _inserted_timestamp
FROM
    liquidation
    LEFT JOIN atoken_meta amc
    ON liquidation.collateral_asset = amc.underlying_address
    LEFT JOIN atoken_meta amd
    ON liquidation.debt_asset = amd.underlying_address qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
