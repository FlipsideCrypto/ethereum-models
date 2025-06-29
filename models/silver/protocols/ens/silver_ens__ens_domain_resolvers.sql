{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','curated','ens']
) }}

WITH base_events AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        topics [0] :: STRING AS topic_0,
        event_name,
        decoded_log AS decoded_flat,
        event_removed,
        tx_succeeded,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        topics [0] :: STRING = '0x335721b01866dc23fbee8b6b2c7b1e14d6f05c28cd35a2c934239f94095602a0' --NewResolver
        AND contract_address IN (
            '0x314159265dd8dbb310642f98f50c066173c1259b',
            '0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e',
            '0x36fc69f0983e536d1787cc83f481581f22cca2a1'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_index,
    event_name,
    origin_from_address AS manager,
    decoded_flat :"node" :: STRING AS node,
    decoded_flat :"resolver" :: STRING AS resolver,
    _log_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    ) }} AS ens_domain_resolvers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base_events
