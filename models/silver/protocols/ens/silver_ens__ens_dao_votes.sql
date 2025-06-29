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
        'ens' AS NAME,
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
        topics [0] :: STRING = '0xb8e138887d0aa13bab447e82de9d5c1777041ecd21ca36ba824ff1e6c07ddda4'
        AND contract_address = '0x323a76393544d5ecca80cd6ef2a560c6a395b7e3'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
votecast AS (
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
        decoded_flat :"proposalId" :: STRING AS proposalId,
        decoded_flat :"reason" :: STRING AS reason,
        decoded_flat :"support" :: STRING AS support_type,
        decoded_flat :"voter" :: STRING AS voter,
        decoded_flat :"weight" :: STRING AS weight,
        weight / pow(
            10,
            18
        ) AS votes,
        CASE
            WHEN support_type = 0 THEN 'against'
            WHEN support_type = 1 THEN 'for'
            WHEN support_type = 2 THEN 'abstain'
        END AS support,
        _log_id,
        _inserted_timestamp
    FROM
        base_events
),
total AS (
    SELECT
        proposalId,
        SUM(votes) AS total_votes
    FROM
        votecast
    GROUP BY
        1
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
    proposalId,
    reason,
    support_type,
    support,
    voter,
    weight AS votes_raw,
    votes,
    CASE
        WHEN total_votes = 0
        OR total_votes IS NULL THEN 0
        ELSE (
            votes / total_votes
        ) * 100
    END AS voting_power_pct,
    _log_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    ) }} AS ens_dao_votes_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    votecast v
    LEFT JOIN total t USING(proposalId)
