{{ config(
    materialized = 'incremental',
    persist_docs ={ "relation": true,
    "columns": true },
    incremental_strategy = 'delete+insert',
    unique_key = "pool_address",
    tags = ['silver_dex','defi','dex','curated']
) }}

WITH forked_factory_pairs AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        contract_address AS factory_address,
        'PairCreated' AS event_name,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTRING(segmented_data [0] :: STRING, 25, 40)) AS pool_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS token0,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS token1,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp,
        -- Platform is now just the factory address for generality
        contract_address AS platform,
        1 AS model_weight,
        'events' AS model_name
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address IN (
            select 
                deployed_contract 
            from 
                {{ ref('silver_dex__factory_addresses') }}
        )
        AND topics [0] = '0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9'
        AND tx_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
)
,
all_pools AS (
    SELECT
        block_number AS creation_block,
        block_timestamp AS creation_time,
        tx_hash AS creation_tx,
        factory_address,
        platform,
        event_name,
        pool_address,
        NULL AS pool_name,
        token0,
        token1,
        NULL AS fee,
        NULL AS tickSpacing,
        ARRAY_CONSTRUCT(
            token0,
            token1
        ) AS tokens,
        _log_id,
        _inserted_timestamp,
        model_weight,
        model_name
    FROM
        forked_factory_pairs
)
SELECT
    creation_block,
    creation_time,
    creation_tx,
    factory_address,
    platform,
    l.address_name,
    l.label,
    l.label_subtype,
    event_name,
    pool_address,
    pool_name,
    token0,
    token1,
    fee,
    tickSpacing,
    tokens,
    _log_id,
    _inserted_timestamp,
    model_weight,
    model_name
FROM
    all_pools
LEFT JOIN {{ ref('core__dim_labels') }} l
    ON pool_address = address
WHERE
    pool_address IS NOT NULL 
qualify(ROW_NUMBER() over(PARTITION BY pool_address
ORDER BY
    model_weight ASC)) = 1
