{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    text_signature,
    hex_signature,
    id
FROM
    {{ source(
        'ethereum_silver',
        'event_signatures_backfill'
    ) }}
