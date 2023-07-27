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
    },
    tags = ['non_realtime']
) }}

SELECT
    contract_address AS feed_address,
    block_number,
    read_result AS latest_answer
FROM
    {{ ref('silver__chainlink_feeds') }}
