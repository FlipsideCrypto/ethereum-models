{{ config(
    materialized = 'view'
) }}

SELECT
    tx_hash,
    block_number,
    block_timestamp,
    from_address,
    to_address,
    value AS eth_value,
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
    modified_timestamp AS _inserted_timestamp,
    input
FROM
    {{ ref('core__fact_traces') }}
WHERE
    TYPE = 'CALL'
    AND eth_value > 0
    AND tx_succeeded
    AND trace_succeeded
