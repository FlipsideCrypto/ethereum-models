{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','lending','curated']
) }}

WITH liquidations AS(

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
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS asset_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS depositor_address,
        origin_from_address AS receiver_address,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS shareamountrepaid,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS amount,
        p.token_address AS silo_market,
        p.protocol_collateral_token_address AS protocol_collateral_token,
        CASE
            WHEN shareamountrepaid > 0 THEN 'debt_token_event'
            ELSE 'collateral_token_event'
        END AS liquidation_event_type,
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
        topics [0] :: STRING = '0xf3fa0eaee8f258c23b013654df25d1527f98a5c7ccd5e951dd77caca400ef972'
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
),
contracts AS (
    SELECT
        *
    FROM
        {{ ref('core__dim_contracts') }}
    WHERE
        address IN (
            SELECT
                asset_address
            FROM
                liquidations
        )
),
debt_token_isolate AS (
    SELECT
        tx_hash,
        asset_address,
        C.symbol as token_symbol,
        liquidation_event_type
    FROM
        liquidations d
        LEFT JOIN contracts C
        ON d.asset_address = C.address
    WHERE
        liquidation_event_type = 'debt_token_event'
)
SELECT
    d.tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    d.contract_address,
    silo_market,
    protocol_collateral_token,
    depositor_address,
    receiver_address,
    d.asset_address AS token_address,
    C.symbol as token_symbol,
    decimals as token_decimals,
    amount AS amount_unadj,
    amount / pow(
        10,
        C.decimals
    ) AS amount,
    i.asset_address AS debt_asset,
    i.token_symbol AS debt_asset_symbol,
    'Silo' AS platform,
    'ethereum' AS blockchain,
    d._log_id,
    d._inserted_timestamp
FROM
    liquidations d
    LEFT JOIN contracts C
    ON d.asset_address = C.address
    LEFT JOIN debt_token_isolate i
    ON d.tx_hash = i.tx_hash
WHERE
    d.liquidation_event_type = 'collateral_token_event' qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    d._inserted_timestamp DESC)) = 1
