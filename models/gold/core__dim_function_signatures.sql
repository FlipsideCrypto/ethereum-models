{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    text_signature,
    bytes_signature,
    id
FROM
    {{ source(
        'ethereum_silver',
        'signatures_backfill'
    ) }}
