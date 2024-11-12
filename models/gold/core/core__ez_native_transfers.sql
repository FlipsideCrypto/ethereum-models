{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(trace_address, from_address, to_address)",
    tags = ['core','non_realtime','reorg']
) }}

SELECT
    tx_hash,
    block_number,
    block_timestamp,
    tx_position,
    trace_index,
    identifier, --deprecate
    trace_address, --new column
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    from_address,
    to_address,
    amount,
    amount_precise_raw,
    amount_precise,
    amount_usd,
    native_transfers_id AS ez_native_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__native_transfers') }}

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP) AS modified_timestamp
    FROM
        {{ this }}
)
{% endif %}
