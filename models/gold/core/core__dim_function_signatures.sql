{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    text_signature,
    bytes_signature,
    id,
    {{ dbt_utils.generate_surrogate_key(
        ['id']
    ) }} AS dim_function_signatures_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ source(
        'ethereum_silver',
        'signatures_backfill'
    ) }}
