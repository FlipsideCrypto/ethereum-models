{{ config(
    materialized = 'view'
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    from_address,
    to_address,
    raw_amount,
    _log_id
FROM
    {{ ref('silver__transfers') }}
