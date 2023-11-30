{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    text_signature,
    hex_signature,
    id,
    {{ dbt_utils.generate_surrogate_key(
        ['id']
    ) }} AS dim_event_signatures_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ source(
        'ethereum_silver',
        'event_signatures_backfill'
    ) }}
