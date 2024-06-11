{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

deposits AS(

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
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS id,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS caller,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS onBehalf,
        utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            ) :: INTEGER
        AS assets,
        'Morpho' AS platform,
        origin_from_address AS depositor_address,
        COALESCE(
            origin_to_address,
            contract_address
        ) AS lending_pool_contract,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
where 
    contract_address = lower('0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb')
AND
    topics[0]::STRING = '0xedf8870433c83823eb071d3df1caa8d008f12f6440918c20d75a3602cda30fe0'

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
    deposit_quantity AS amount_unadj,
    deposit_quantity / pow(
        10,
        atoken_meta.underlying_decimals
    ) AS amount,
    LOWER(
        depositor_address
    ) AS depositor_address,
    LOWER(
        lending_pool_contract
    ) AS lending_pool_contract,
    morpho_version AS platform,
    atoken_meta.underlying_symbol AS symbol,
    'ethereum' AS blockchain,
    _log_id,
    _inserted_timestamp
FROM
    deposits
    LEFT JOIN atoken_meta
    ON deposits.morpho_market = atoken_meta.underlying_address qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
