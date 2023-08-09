{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'CHAINLINK',
                'PURPOSE': 'ORACLE'
            }
        }
    }
) }}

SELECT
    feed_address,
    feed_name,
    feed_category,
    feed_added AS feed_added_date,
    created_block_number
FROM
    {{ ref('silver__chainlink_feeds_seed') }}
