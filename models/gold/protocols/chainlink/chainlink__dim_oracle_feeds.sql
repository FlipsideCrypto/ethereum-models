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
    created_block_number,
    COALESCE (
        chainlink_feed_details_id,
        {{ dbt_utils.generate_surrogate_key(
            ['feed_address']
        ) }}
    ) AS dim_oracle_feeds_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__chainlink_feed_details') }}
