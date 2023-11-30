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
    block_number,
    read_result AS latest_answer,
    COALESCE (
        chainlink_feeds_id,
        {{ dbt_utils.generate_surrogate_key(
            ['block_number', 'contract_address']
        ) }}
    ) AS fact_oracle_feeds_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__chainlink_feeds') }}
