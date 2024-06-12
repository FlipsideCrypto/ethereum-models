{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH base_evt AS (

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
        topics [0] :: STRING AS topic_0,
        topics [1] :: STRING AS topic_1,
        topics [2] :: STRING AS topic_2,
        topics [3] :: STRING AS topic_3,
        decoded_flat,
        decoded_flat :sender :: STRING AS donator_address,
        decoded_flat :serviceIds AS service_ids,
        decoded_flat :amounts AS amounts_unadj,
        TRY_TO_NUMBER(
            decoded_flat :donation :: STRING
        ) AS donation_unadj,
        donation_unadj / pow(
            10,
            18
        ) :: FLOAT AS donation_adj,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        contract_address = '0xa0da53447c0f6c4987964d8463da7e6628b30f82' --Treasury
        AND topic_0 = '0xb7c3b13b911bdc71cbcf384e470cad657b7da964c892dc2fdba1432ad64d8be0' --DonateToServicesETH
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
evt_flat AS (
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
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        decoded_flat,
        donator_address,
        service_ids,
        amounts_unadj,
        donation_unadj,
        donation_adj,
        TRY_TO_NUMBER(
            f1.value :: STRING
        ) AS service_id,
        TRY_TO_NUMBER(
            f2.value :: STRING
        ) AS amount_unadj,
        amount_unadj / pow(
            10,
            18
        ) :: FLOAT AS amount_adj,
        _log_id,
        _inserted_timestamp
    FROM
        base_evt,
        LATERAL FLATTEN(
            input => service_ids
        ) AS f1,
        LATERAL FLATTEN(
            input => amounts_unadj
        ) AS f2
    WHERE
        f1.index = f2.index
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
    topic_0,
    topic_1,
    topic_2,
    topic_3,
    decoded_flat,
    donator_address,
    service_id,
    amount_unadj,
    amount_adj AS amount,
    donation_unadj AS total_donation_unadj,
    donation_adj AS total_donation,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS service_donations_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    evt_flat
