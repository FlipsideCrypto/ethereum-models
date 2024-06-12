{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'OLAS, AUTONOLAS, VALORY',
    'PURPOSE': 'AI, SERVICES' } } }
) }}

SELECT
    s.block_number,
    s.block_timestamp,
    s.tx_hash,
    s.origin_function_signature,
    s.origin_from_address,
    s.origin_to_address,
    s.contract_address,
    s.event_index,
    s.event_name,
    s.donor_address,
    s.service_id,
    m.name,
    m.description,
    s.amount_unadj AS eth_amount_unadj,
    s.amount AS eth_amount,
    eth_amount * p.price AS eth_amount_usd,
    s.total_donation_unadj AS total_eth_unadj,
    s.total_donation AS total_eth,
    total_eth * p.price AS total_eth_usd,
    s.service_donations_id AS ez_service_donations_id,
    s.inserted_timestamp,
    s.modified_timestamp
FROM
    {{ ref('silver_olas__service_donations') }}
    s
    LEFT JOIN {{ ref('silver_olas__registry_metadata') }}
    m
    ON m.contract_address = '0x48b6af7b12c71f09e2fc8af4855de4ff54e775ca'
    AND s.service_id = m.registry_id
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p
    ON p.token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    AND DATE_TRUNC(
        'hour',
        s.block_timestamp
    ) = p.hour