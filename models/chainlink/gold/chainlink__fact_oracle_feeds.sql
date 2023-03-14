{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'CHAINLINK',
    'PURPOSE': 'ORACLE' }} }
) }}

SELECT
    feed_address,
    block_number,
    answer AS latest_answer,
    updated_at
FROM
    {{ ref('silver__chainlink_feeds') }}
