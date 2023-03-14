{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'CHAINLINK',
    'PURPOSE': 'ORACLE' }} }
) }}

SELECT
    A.feed_address,
    block_number,
    block_timestamp,
    feed_name,
    answer AS latest_answer_unadj,
    latest_answer_unadj / pow(10, COALESCE(decimals, 18)) AS latest_answer_adj,
    feed_category,
    feed_added AS feed_added_date,
    created_block_number,
    updated_at
FROM
    {{ ref('silver__chainlink_feeds') }} A
    JOIN {{ ref('silver__chainlink_feeds_seed') }} USING (
        feed_address
    )
    LEFT JOIN {{ ref('silver__contracts') }}
    ON A.feed_address = address
