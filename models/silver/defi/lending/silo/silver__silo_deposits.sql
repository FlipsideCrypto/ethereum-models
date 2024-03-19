{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH deposits AS(

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        l.contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS asset_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS depositor_address,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS amount_unadj,
        p.token_address AS silo_market,
        l._log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
        INNER JOIN {{ ref('silver__silo_pools') }}
        p
        ON l.contract_address = p.silo_address
    WHERE
        topics [0] :: STRING = '0xdd160bb401ec5b5e5ca443d41e8e7182f3fe72d70a04b9c0ba844483d212bcb5'
        AND tx_status = 'SUCCESS' --excludes failed txs

{% if is_incremental() %}
AND l._inserted_timestamp >= (
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
    d.contract_address,
    silo_market,
    asset_address AS token_address,
    C.symbol as token_symbol,
    amount_unadj,
    amount_unadj / pow(
        10,
        C.decimals
    ) AS amount,
    depositor_address,
    'Silo' AS platform,
    'ethereum' AS blockchain,
    d._log_id,
    d._inserted_timestamp
FROM
    deposits d
    LEFT JOIN {{ ref('silver__contracts') }} C
    ON d.asset_address = C.address qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    d._inserted_timestamp DESC)) = 1
