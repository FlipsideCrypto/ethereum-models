{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
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
        topics [0] :: STRING IN (
            '0xca6abbe9d7f11422cb6ca7629fbf6fe9efb1c621f71ce8f02b9f2a230097404f',
            '0x69e37f151eb98a09618ddaa80c8cfaf1ce5996867c489f45b555b412271ebf27',
            '0xb3d987963d01b2f68493b4bdb130988f157ea43070d4ad840fee0466ed9370d9'
        )
        AND contract_address IN (
            '0x283af0b28c62c092c9727f1ee09c02ca627eb7f5',
            '0x253553366da8546fc250f225fe3d25d0c782303b',
            '0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85',
            '0xb22c1c159d12461ea124b0deb4b5b93020e6ad16',
            '0xf0ad5cad05e10572efceb849f6ff0c68f9700455',
            '0x82994379b1ec951c8e001dfcec2a7ce8f4f39b97'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
name_registered AS (
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
        TRY_TO_NUMBER(
            decoded_flat :"cost" :: STRING
        ) AS cost_raw,
        cost_raw / pow(
            10,
            18
        ) AS cost_adj,
        decoded_flat :"expires" :: STRING AS expires,
        TRY_TO_TIMESTAMP(expires) AS expires_timestamp,
        decoded_flat :"label" :: STRING AS label,
        decoded_flat :"name" :: STRING AS NAME,
        decoded_flat :"owner" :: STRING AS owner,
        NULL AS premium_raw,
        NULL AS premium_adj,
        _log_id,
        _inserted_timestamp
    FROM
        base_events
    WHERE
        topic_0 = '0xca6abbe9d7f11422cb6ca7629fbf6fe9efb1c621f71ce8f02b9f2a230097404f' --v1
    UNION ALL
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
        TRY_TO_NUMBER(
            decoded_flat :"baseCost" :: STRING
        ) AS cost_raw,
        cost_raw / pow(
            10,
            18
        ) AS cost_adj,
        decoded_flat :"expires" :: STRING AS expires,
        TRY_TO_TIMESTAMP(expires) AS expires_timestamp,
        decoded_flat :"label" :: STRING AS label,
        decoded_flat :"name" :: STRING AS NAME,
        decoded_flat :"owner" :: STRING AS owner,
        TRY_TO_NUMBER(
            decoded_flat :"premium" :: STRING
        ) AS premium_raw,
        premium_raw / pow(
            10,
            18
        ) AS premium_adj,
        _log_id,
        _inserted_timestamp
    FROM
        base_events
    WHERE
        topic_0 = '0x69e37f151eb98a09618ddaa80c8cfaf1ce5996867c489f45b555b412271ebf27' --v2
),
new_resolver AS (
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
        node,
        resolver,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver_ens__ens_domain_resolvers') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '18 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
resolver_paired_evt_index AS (
    SELECT
        n.tx_hash,
        n.event_index AS nameregistered_evt_index,
        r.event_index AS newresolver_evt_index
    FROM
        name_registered n
        LEFT JOIN new_resolver r
        ON n.tx_hash = r.tx_hash
    WHERE
        r.event_index = (
            SELECT
                MAX(
                    rr.event_index
                )
            FROM
                new_resolver rr
            WHERE
                rr.tx_hash = n.tx_hash
                AND rr.event_index < n.event_index
        )
),
tokenid_registered AS (
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
        decoded_flat :"expires" :: STRING AS expires,
        decoded_flat :"id" :: STRING AS token_id,
        decoded_flat :"owner" :: STRING AS owner,
        _log_id,
        _inserted_timestamp
    FROM
        base_events
    WHERE
        topic_0 = '0xb3d987963d01b2f68493b4bdb130988f157ea43070d4ad840fee0466ed9370d9'
),
tokenid_paired_evt_index AS (
    SELECT
        n.tx_hash,
        n.event_index AS nameregistered_evt_index,
        t.event_index AS tokenid_evt_index
    FROM
        name_registered n
        LEFT JOIN tokenid_registered t
        ON n.tx_hash = t.tx_hash
    WHERE
        t.event_index = (
            SELECT
                MAX(
                    tr.event_index
                )
            FROM
                tokenid_registered tr
            WHERE
                tr.tx_hash = n.tx_hash
                AND tr.event_index < n.event_index
        )
)
SELECT
    n.block_number,
    n.block_timestamp,
    n.tx_hash,
    n.origin_function_signature,
    n.origin_from_address,
    n.origin_to_address,
    n.contract_address,
    n.event_index,
    n.event_name,
    n.origin_from_address AS manager,
    n.owner,
    NAME,
    label,
    node,
    token_id,
    resolver,
    cost_raw,
    cost_adj AS cost,
    premium_raw,
    premium_adj AS premium,
    n.expires,
    expires_timestamp,
    n._log_id,
    n._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['n.tx_hash', 'n.event_index']
    ) }} AS ens_domain_registrations_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    name_registered n
    LEFT JOIN resolver_paired_evt_index p
    ON n.tx_hash = p.tx_hash
    AND n.event_index = p.nameregistered_evt_index
    LEFT JOIN new_resolver r
    ON r.tx_hash = p.tx_hash
    AND r.event_index = p.newresolver_evt_index
    LEFT JOIN tokenid_paired_evt_index i
    ON n.tx_hash = i.tx_hash
    AND n.event_index = i.nameregistered_evt_index
    LEFT JOIN tokenid_registered t
    ON t.tx_hash = i.tx_hash
    AND t.event_index = i.tokenid_evt_index
