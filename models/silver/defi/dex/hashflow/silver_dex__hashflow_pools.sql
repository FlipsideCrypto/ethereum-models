{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "pool_address",
    tags = ['curated']
) }}

WITH contract_deployments AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        from_address AS deployer_address,
        to_address AS contract_address,
        concat_ws(
            '-',
            block_number,
            tx_position,
            CONCAT(
                TYPE,
                '_',
                trace_address
            )
        ) AS _call_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        from_address IN (
            '0x63ae536fec0b57bdeb1fd6a893191b4239f61bff',
            '0xc11a1e6fde432df9467d6d1a5454b54a63b86c8c',
            '0x596d32f9b7c1f2e73f5071c66b5e336e27d00da4'
        )
        AND TYPE ILIKE 'create%'
        AND tx_status = 'SUCCESS'
        AND trace_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY to_address
ORDER BY
    block_timestamp ASC)) = 1
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    deployer_address,
    contract_address AS pool_address,
    _call_id,
    _inserted_timestamp
FROM
    contract_deployments
