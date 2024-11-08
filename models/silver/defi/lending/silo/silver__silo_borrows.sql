{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH borrows AS(

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
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS borrow_address,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS amount,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS collateral_only,
        p.token_address AS silo_market,
        CONCAT(
            l.tx_hash,
            '-',
            l.event_index
        ) AS _log_id,
        l.modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
        INNER JOIN {{ ref('silver__silo_pools') }}
        p
        ON l.contract_address = p.silo_address
    WHERE
        topics [0] :: STRING = '0x312a5e5e1079f5dda4e95dbbd0b908b291fd5b992ef22073643ab691572c5b52'
        AND tx_succeeded --excludes failed txs

{% if is_incremental() %}
AND l.modified_timestamp >= (
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
    c.decimals as token_decimals,
    amount AS amount_unadj,
    amount / pow(
        10,
        C.decimals
    ) AS amount,
    borrow_address AS borrower,
    'Silo' AS platform,
    'ethereum' AS blockchain,
    d._log_id,
    d._inserted_timestamp
FROM
    borrows d
    LEFT JOIN {{ ref('silver__contracts') }} C
    ON d.asset_address = C.address qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    d._inserted_timestamp DESC)) = 1
