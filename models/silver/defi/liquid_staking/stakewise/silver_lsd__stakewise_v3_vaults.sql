{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "vault_address",
    tags = ['curated']
) }}

WITH factories AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        from_address AS deployer_address,
        to_address AS factory_address,
        concat_ws(
            '-',
            block_number,
            tx_position,
            CONCAT(
                type,
                '_',
                trace_address
            )
        ) AS _call_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        from_address = '0x229f53ef905545aa53a721d82dbfe4ced7aff65d' --StakeWise: Deployer 1
        AND TYPE ILIKE 'create%'
        AND tx_succeeded
        AND trace_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY to_address
ORDER BY
    _inserted_timestamp DESC)) = 1
),
vaults AS (
    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        l.event_index,
        l.contract_address,
        deployer_address,
        factory_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS admin,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS vault,
        _log_id,
        l._inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
        INNER JOIN factories f
        ON l.contract_address = f.factory_address
    WHERE
        topics [0] :: STRING = '0x0d606510f33b5e566ed1ca2b9e88d388ab81cea532909665d725b33134516aff' --VaultCreated
        AND tx_succeeded

{% if is_incremental() %}
AND l._inserted_timestamp >= (
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
    contract_address,
    event_index,
    deployer_address,
    factory_address,
    admin AS admin_address,
    vault AS vault_address,
    _log_id,
    _inserted_timestamp
FROM
    vaults qualify(ROW_NUMBER() over (PARTITION BY vault_address
ORDER BY
    _inserted_timestamp DESC)) = 1
