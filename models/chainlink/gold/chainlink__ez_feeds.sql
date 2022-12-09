{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    contract_address AS feed_address,
    block_number,
    feed_name,
    read_result AS latest_answer_unadj,
    latest_answer_unadj / pow(
        10,
        decimals
    ) AS latest_answer_adj,
    feed_category,
    feed_added AS feed_added_date,
    created_block_number
FROM
    {{ ref('silver__chainlink_feeds') }}
    JOIN {{ ref('silver__chainlink_feeds_seed') }}
    ON contract_address = feed_address
    JOIN {{ ref('silver__contracts') }}
    ON contract_address = address
