{{ config(
    materialized = 'incremental',
    unique_key = '_id',
    incremental_strategy = 'delete+insert',
    tags = ['non_realtime']
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
        decoded_flat,
        event_removed,
        tx_status,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        topics [0] :: STRING IN (
            '0x3da24c024582931cfaf8267d8ed24d13a82a8068d5bd337d30ec45cea4e506ae',
            --NameRenewed
            '0x335721b01866dc23fbee8b6b2c7b1e14d6f05c28cd35a2c934239f94095602a0',
            --NewResolver
            '0xce0457fe73731f824cc272376169235128c118b49d344817417c6d108d155e82',
            --NewOwner
            '0x65412581168e88a1e60c6459d7f44ae83ad0832e670826c05a4e2476b57af752',
            --AddressChanged
            '0xb7d29e911041e8d9b843369e890bcb72c9388692ba48b65ac54e7214c4c348f7',
            --NameChanged
            '0xd8c9334b1a9c2f9da342a0a2b32629c1a229b6445dad78947f674b44444a7550',
            --TextChanged
            '0x6ada868dd3058cf77a48a74489fd7963688e5464b2b0fa957ace976243270e92' --ReverseClaimed --add'l info needed from txns/traces
            --add in logic/events for expired, no longer current/owned ens domains
        )
        AND contract_address IN (
            '0x253553366da8546fc250f225fe3d25d0c782303b',
            '0x283af0b28c62c092c9727f1ee09c02ca627eb7f5',
            '0x82994379b1ec951c8e001dfcec2a7ce8f4f39b97',
            '0xa271897710a2b22f7a5be5feacb00811d960e0b8',
            '0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85',
            '0xfac7bea255a6990f749363002136af6556b31e04',
            '0xf0ad5cad05e10572efceb849f6ff0c68f9700455',
            '0xb22c1c159d12461ea124b0deb4b5b93020e6ad16',
            '0x314159265dd8dbb310642f98f50c066173c1259b',
            '0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e',
            '0x231b0ee14048e9dccd1d247744d114a4eb5e8e63',
            '0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41',
            '0xa58e81fe9b61b5c3fe2afd33cf304c454abfc7cb'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '24 hours'
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
        manager,
        owner,
        NAME,
        label,
        cost_raw,
        cost,
        premium_raw,
        premium,
        expires,
        expires_timestamp,
        LAG(node) over (
            PARTITION BY label
            ORDER BY
                block_timestamp ASC nulls last
        ) AS prev_node,
        LAG(resolver) over (
            PARTITION BY label
            ORDER BY
                block_timestamp ASC nulls last
        ) AS prev_resolver,
        COALESCE(
            node,
            prev_node
        ) AS registered_node,
        COALESCE(
            resolver,
            prev_resolver
        ) AS registered_resolver,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver_ens__ens_domain_registrations') }}

{% if is_incremental() %}
WHERE
    NAME NOT IN (
        SELECT
            DISTINCT NAME
        FROM
            {{ this }}
    )
{% endif %}
),
name_renewed AS (
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
        decoded_flat :"name" :: STRING AS NAME,
        decoded_flat :"label" :: STRING AS label,
        TRY_TO_NUMBER(
            decoded_flat :"cost" :: STRING
        ) AS cost_raw,
        cost_raw / pow(
            10,
            18
        ) AS cost,
        decoded_flat :"expires" :: STRING AS expires,
        TRY_TO_TIMESTAMP(expires) AS expires_timestamp,
        _log_id,
        _inserted_timestamp
    FROM
        base_events
    WHERE
        topic_0 = '0x3da24c024582931cfaf8267d8ed24d13a82a8068d5bd337d30ec45cea4e506ae' qualify(ROW_NUMBER() over (PARTITION BY label
    ORDER BY
        block_timestamp DESC)) = 1
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
        origin_from_address AS manager,
        decoded_flat :"node" :: STRING AS node,
        decoded_flat :"resolver" :: STRING AS resolver,
        _log_id,
        _inserted_timestamp
    FROM
        base_events
    WHERE
        topic_0 = '0x335721b01866dc23fbee8b6b2c7b1e14d6f05c28cd35a2c934239f94095602a0' qualify(ROW_NUMBER() over (PARTITION BY node
    ORDER BY
        block_timestamp DESC)) = 1
),
new_owner AS (
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
        decoded_flat :"label" :: STRING AS label,
        decoded_flat :"node" :: STRING AS node,
        decoded_flat :"owner" :: STRING AS owner,
        _log_id,
        _inserted_timestamp
    FROM
        base_events
    WHERE
        topic_0 = '0xce0457fe73731f824cc272376169235128c118b49d344817417c6d108d155e82' qualify(ROW_NUMBER() over (PARTITION BY label
    ORDER BY
        block_timestamp DESC)) = 1
),
FINAL AS (
SELECT
    rd.block_number AS last_registered_block,
    rd.block_timestamp AS last_registered_timestamp,
    rd.tx_hash AS last_registered_tx_hash,
    rd.contract_address,
    GREATEST(rd.block_timestamp, COALESCE(rw.block_timestamp, 0 :: TIMESTAMP), COALESCE(r.block_timestamp, 0 :: TIMESTAMP), COALESCE(o.block_timestamp, 0 :: TIMESTAMP)) AS last_updated,
    CASE
        WHEN last_updated = 
            rd.block_timestamp THEN 'rd'
        WHEN last_updated = COALESCE(
            rw.block_timestamp,
            0 :: TIMESTAMP
        ) THEN 'rw'
        WHEN last_updated = COALESCE(
            r.block_timestamp,
            0 :: TIMESTAMP
        ) THEN 'r'
        WHEN last_updated = COALESCE(
            o.block_timestamp,
            0 :: TIMESTAMP
        ) THEN 'o'
    END AS latest_record_type,
    CASE
        WHEN latest_record_type = 'rd' THEN rd.manager
        WHEN latest_record_type = 'rw' THEN rw.manager
        WHEN latest_record_type = 'r' THEN r.manager
        WHEN latest_record_type = 'o' THEN o.manager
    END AS manager,
    CASE
        WHEN latest_record_type = 'rd' THEN rd.owner
        WHEN latest_record_type <> 'rd' THEN o.owner
    END AS owner,
    rd.name,
    rd.label,
    registered_node AS node,
    rd.cost AS last_registered_cost,
    COALESCE(
        rd.premium,
        0
    ) AS last_registered_premium,
    rw.cost AS renewal_cost,
    GREATEST(rd.expires_timestamp, COALESCE(rw.expires_timestamp, 0 :: TIMESTAMP)) AS expiration_timestamp,
    CASE
        WHEN expiration_timestamp < CURRENT_TIMESTAMP THEN TRUE
        ELSE FALSE
    END AS expired,
    CASE
        WHEN latest_record_type = 'rd' THEN rd.registered_resolver
        WHEN latest_record_type <> 'rd' THEN r.resolver
    END AS resolver,
    {# ENS_SET, --reverse record set? #}
    {{ dbt_utils.generate_surrogate_key(
        ['rd.name']
    ) }} AS _id,
    GREATEST(
        rd._inserted_timestamp,
        COALESCE(
            rw._inserted_timestamp,
            0 :: TIMESTAMP,
            COALESCE(
                r._inserted_timestamp,
                0 :: TIMESTAMP,
                COALESCE(
                    o._inserted_timestamp,
                    0 :: TIMESTAMP
                )
            )
        )
    ) AS _inserted_timestamp
FROM
    name_registered rd
    LEFT JOIN name_renewed rw
    ON rd.label = rw.label
    LEFT JOIN new_resolver r
    ON rd.registered_node = r.node
    LEFT JOIN new_owner o
    ON rd.label = o.label qualify(ROW_NUMBER() over (PARTITION BY rd.label
ORDER BY
    last_registered_timestamp DESC)) = 1
)

SELECT
    last_registered_block,
    last_registered_timestamp,
    last_registered_tx_hash,
    contract_address,
    manager,
    owner,
    name,
    label,
    node,
    last_registered_cost,
    last_registered_premium,
    renewal_cost,
    expiration_timestamp,
    expired,
    resolver,
    last_updated,
    _id,
    _inserted_timestamp
FROM
    FINAL
