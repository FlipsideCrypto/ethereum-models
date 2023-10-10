{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    tags = ['non_realtime']
) }}

WITH pool_events AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS newBorn,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS baseToken,
        CONCAT('0x', SUBSTR(segmented_data [2] :: STRING, 25, 40)) AS quoteToken,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref ('silver__logs') }}
    WHERE
        contract_address IN (
            '0xbd337924f000dceb119153d4d3b1744b22364d25',
            '0xe1b5d7a770cb1b40c859a52696e7e3dd1c57b0ba',
            '0x3a97247df274a17c59a3bd12735ea3fcdfb49950'
        ) --DODOZoo
        AND topics [0] :: STRING = '0x5c428a2e12ecaa744a080b25b4cda8b86359c82d726575d7d747e07708071f93' --DODOBirth

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
pool_calls AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        from_address AS deployer_address,
        to_address AS pool_address,
        base_token,
        quote_token,
        base_token_symbol,
        quote_token_symbol,
        _call_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__traces') }} t
    INNER JOIN {{ref('silver__dodo_v1_pools')}} s ON t.to_address = s.pool_address
    WHERE
        from_address IN (
            '0x5e5a7b76462e4bdf83aa98795644281bdba80b88',
            '0x17dbfa501f2f376d092fa69d3223a09bba4efdf7'
        ) --DODO deployer contracts
        AND TYPE ILIKE 'create%'
        AND tx_status = 'SUCCESS'
        AND trace_status = 'SUCCESS'
        AND block_timestamp :: DATE >= '2020-08-07'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    contract_address,
    newBorn AS pool_address,
    baseToken AS base_token,
    quoteToken AS quote_token,
    _log_id AS _id,
    _inserted_timestamp
FROM
    pool_events
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    NULL AS event_index,
    deployer_address AS contract_address,
    pool_address,
    base_token,
    quote_token,
    _call_id AS _id,
    _inserted_timestamp
FROM
    pool_calls
