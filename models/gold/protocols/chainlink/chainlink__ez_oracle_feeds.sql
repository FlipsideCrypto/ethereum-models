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
    contract_address AS feed_address,
    A.block_number,
    block_timestamp,
    feed_name,
    read_result AS latest_answer_unadj,
    latest_answer_unadj / pow(
        10,
        decimals
    ) AS latest_answer_adj,
    feed_category,
    feed_added AS feed_added_date,
    created_block_number,
    COALESCE (
        chainlink_feeds_id,
        {{ dbt_utils.generate_surrogate_key(
            ['block_number', 'contract_address']
        ) }}
    ) AS ez_oracle_feeds_id,
    greatest(
        a.inserted_timestamp,
        b.inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    greatest(
        a.modified_timestamp,
        b.modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
    
FROM
    {{ ref('silver__chainlink_feeds') }} A
    JOIN {{ ref('silver__chainlink_feeds_seed') }}
    ON contract_address = feed_address
    JOIN {{ ref('silver__contracts') }}
    ON contract_address = address
    JOIN {{ ref('silver__blocks') }}
    b
    ON A.block_number = b.block_number
