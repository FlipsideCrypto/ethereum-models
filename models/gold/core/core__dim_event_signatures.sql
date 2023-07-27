{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['non_realtime']
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
