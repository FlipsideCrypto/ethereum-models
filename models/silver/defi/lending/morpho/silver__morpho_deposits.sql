{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

with morpho_metas as (
    SELECT
        block_number,
        tx_hash,
        token_address,
        token_name,
        token_symbol,
        token_decimals,
        contract_address,
        underlying_asset,
        underlying_name,
        underlying_symbol,
        underlying_decimals
    from
        {{ ref('silver__morpho_vaults') }}
),
deposits AS(

    SELECT
        l.tx_hash,
        l.block_number,
        l.block_timestamp,
        l.event_index,
        l.origin_from_address,
        l.origin_to_address,
        l.origin_function_signature,
        l.contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS sender,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS owner,
        utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            ) :: INTEGER
        AS assets,
        utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            ) :: INTEGER
        AS shares,
        m.token_address,
        m.token_name,
        m.token_symbol,
        m.underlying_asset,
        m.underlying_name,
        m.underlying_symbol,
        m.underlying_decimals,
        origin_from_address AS depositor_address,
        COALESCE(
            l.origin_to_address,
            l.contract_address
        ) AS lending_pool_contract,
        l._log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }} l 
    INNER JOIN
        morpho_metas m
    ON
        l.contract_address = m.token_address
    WHERE
        topics[0]::STRING = '0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7'
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
    token_address as protocol_market,
    lower(owner) as depositor,
    assets AS amount_unadj,
    assets / pow(
        10,
        underlying_decimals
    ) AS amount,
    LOWER(
        lending_pool_contract
    ) AS lending_pool_contract,
    'Morpho Blue' AS platform,
    underlying_symbol AS symbol,
    'ethereum' AS blockchain,
    _log_id,
    _inserted_timestamp
FROM
    deposits qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
