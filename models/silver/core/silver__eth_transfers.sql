{{ config(
    materialized = 'view'
) }}

SELECT
    tx_hash,
    block_number,
    block_timestamp,
    from_address,
    to_address,
    VALUE AS eth_value,
    identifier,
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
    modified_timestamp AS _inserted_timestamp,
    input
FROM
    {{ ref('core__fact_traces') }}
WHERE
    TYPE = 'CALL'
    AND VALUE > 0
    AND tx_status = 'SUCCESS'
    AND trace_status = 'SUCCESS'
