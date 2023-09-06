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
            '0x335721b01866dc23fbee8b6b2c7b1e14d6f05c28cd35a2c934239f94095602a0',
            --NewResolver
            '0xce0457fe73731f824cc272376169235128c118b49d344817417c6d108d155e82',
            --NewOwner
            '0x65412581168e88a1e60c6459d7f44ae83ad0832e670826c05a4e2476b57af752',
            --AddressChanged
            '0xb7d29e911041e8d9b843369e890bcb72c9388692ba48b65ac54e7214c4c348f7',
            --NameChanged
            '0xd8c9334b1a9c2f9da342a0a2b32629c1a229b6445dad78947f674b44444a7550' --TextChanged
        )
        AND contract_address IN (
            '0x314159265dd8dbb310642f98f50c066173c1259b',
            '0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e',
            '0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41',
            '0x231b0ee14048e9dccd1d247744d114a4eb5e8e63'
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
        token_id,
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
        manager,
        NAME,
        label,
        cost_raw,
        cost,
        expires,
        expires_timestamp,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver_ens__ens_domain_renewals') }}
        qualify(ROW_NUMBER() over (PARTITION BY label
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
        block_timestamp DESC, event_index DESC)) = 1
),
name_wrapped AS (
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
        NAME,
        name_clean,
        top_level_domain,
        ens_domain,
        node,
        label,
        manager,
        owner,
        OPERATOR,
        token_id,
        expiry,
        expiry_timestamp,
        fuses,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver_ens__ens_domain_wrapped') }}
    WHERE
        event_name = 'NameWrapped' qualify(ROW_NUMBER() over (PARTITION BY NAME
    ORDER BY
        block_timestamp DESC)) = 1
),
name_set AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        set_address,
        set_ens_name,
        set_ens_name_clean,
        set_ens_name_clean2,
        subdomain2,
        subdomain1,
        second_level_domain,
        top_level_domain,
        _inserted_timestamp
    FROM
        {{ ref('silver_ens__ens_domain_set') }}
        qualify(ROW_NUMBER() over (PARTITION BY set_ens_name
    ORDER BY
        block_timestamp DESC)) = 1
),
transfers AS (
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
        OPERATOR,
        from_address,
        to_address,
        token_id,
        token_value,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver_ens__ens_domain_transfers') }}
        qualify(ROW_NUMBER() over (PARTITION BY token_id
    ORDER BY
        block_timestamp DESC, event_index DESC)) = 1
),
text_changed AS (
    SELECT
        latest_block,
        latest_timestamp,
        manager,
        node,
        profile_info,
        _inserted_timestamp
    FROM
        {{ ref('silver_ens__ens_domain_textchanged') }}
),
FINAL AS (
    SELECT
        rd.block_number AS last_registered_block,
        rd.block_timestamp AS last_registered_timestamp,
        rd.tx_hash AS last_registered_tx_hash,
        rd.contract_address,
        GREATEST(
            rd.block_timestamp,
            COALESCE(
                rw.block_timestamp,
                0 :: TIMESTAMP
            ),
            COALESCE(
                r.block_timestamp,
                0 :: TIMESTAMP
            ),
            COALESCE(
                o.block_timestamp,
                0 :: TIMESTAMP
            ),
            COALESCE(
                w.block_timestamp,
                0 :: TIMESTAMP
            ),
            COALESCE(
                t.block_timestamp,
                0 :: TIMESTAMP
            ),
            COALESCE(
                x.latest_timestamp,
                0 :: TIMESTAMP
            )
        ) AS last_updated,
        CASE
            WHEN last_updated = rd.block_timestamp THEN 'rd'
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
            WHEN last_updated = COALESCE(
                w.block_timestamp,
                0 :: TIMESTAMP
            ) THEN 'w'
            WHEN last_updated = COALESCE(
                t.block_timestamp,
                0 :: TIMESTAMP
            ) THEN 't'
            WHEN last_updated = COALESCE(
                x.latest_timestamp,
                0 :: TIMESTAMP
            ) THEN 'x'
        END AS latest_record_type,
        CASE
            WHEN latest_record_type = 'rd' THEN rd.manager
            WHEN latest_record_type = 'rw' THEN rw.manager
            WHEN latest_record_type = 'r' THEN r.manager
            WHEN latest_record_type = 'o' THEN o.manager
            WHEN latest_record_type = 'w' THEN w.manager
            WHEN latest_record_type = 't' THEN t.to_address
            WHEN latest_record_type = 'x' THEN x.manager
        END AS manager,
        CASE
            WHEN latest_record_type = 'rd' THEN rd.owner
            WHEN latest_record_type = 'w' THEN w.owner
            WHEN latest_record_type = 't' THEN t.to_address
            WHEN latest_record_type = 'o'
            AND o.owner <> '0xd4416b13d2b3a9abae7acd5d6c2bbdbe25686401' THEN o.owner
            WHEN (
                latest_record_type = 'o'
                AND o.owner = '0xd4416b13d2b3a9abae7acd5d6c2bbdbe25686401'
            )
            OR latest_record_type NOT IN (
                'rd',
                'w',
                't',
                'o'
            ) THEN COALESCE(
                t.to_address,
                w.owner,
                rd.owner
            )
            ELSE NULL
        END AS new_owner,
        CASE
            WHEN new_owner IS NULL
            OR new_owner = '0xd4416b13d2b3a9abae7acd5d6c2bbdbe25686401' THEN rd.owner
            ELSE new_owner
        END AS owner,
        set_address,
        CASE
            WHEN set_address IS NULL THEN FALSE
            ELSE TRUE
        END AS ens_set,
        rd.name,
        rd.label,
        COALESCE(
            registered_node,
            w.node
        ) AS node,
        COALESCE(
            rd.token_id,
            w.token_id
        ) AS token_id,
        rd.cost AS last_registered_cost,
        COALESCE(
            rd.premium,
            0
        ) AS last_registered_premium,
        rw.cost AS renewal_cost,
        GREATEST(
            rd.expires_timestamp,
            COALESCE(
                rw.expires_timestamp,
                0 :: TIMESTAMP
            )
        ) AS expiration_timestamp,
        CASE
            WHEN expiration_timestamp < CURRENT_TIMESTAMP THEN TRUE
            ELSE FALSE
        END AS expired,
        CASE
            WHEN latest_record_type <> 'rd' THEN r.resolver
            ELSE rd.registered_resolver
        END AS resolver,
        profile_info AS profile,
        {{ dbt_utils.generate_surrogate_key(
            ['rd.name','rd.label']
        ) }} AS _id,
        GREATEST(
            rd._inserted_timestamp,
            COALESCE(
                rw._inserted_timestamp,
                0 :: TIMESTAMP
            ),
            COALESCE(
                r._inserted_timestamp,
                0 :: TIMESTAMP
            ),
            COALESCE(
                o._inserted_timestamp,
                0 :: TIMESTAMP
            ),
            COALESCE(
                w._inserted_timestamp,
                0 :: TIMESTAMP
            ),
            COALESCE(
                t._inserted_timestamp,
                0 :: TIMESTAMP
            ),
            COALESCE(
                x._inserted_timestamp,
                0 :: TIMESTAMP
            )
        ) AS _inserted_timestamp
    FROM
        name_registered rd
        LEFT JOIN name_renewed rw
        ON rd.label = rw.label
        LEFT JOIN new_resolver r
        ON rd.registered_node = r.node
        LEFT JOIN new_owner o
        ON rd.label = o.label
        LEFT JOIN name_wrapped w
        ON rd.label = w.label
        LEFT JOIN name_set s
        ON rd.name = s.set_ens_name_clean
        LEFT JOIN transfers t
        ON rd.token_id = t.token_id
        LEFT JOIN text_changed x
        ON rd.registered_node = x.node
)
SELECT
    last_registered_block,
    last_registered_timestamp,
    last_registered_tx_hash,
    contract_address AS last_registered_contract,
    manager,
    owner,
    set_address,
    ens_set,
    NAME AS ens_domain,
    ARRAY_AGG(ens_subdomain) AS ens_subdomains,
    label,
    node,
    token_id,
    last_registered_cost,
    last_registered_premium,
    renewal_cost,
    expiration_timestamp,
    expired,
    f.resolver,
    profile,
    last_updated,
    latest_record_type,
    'ethereum' AS last_registered_blockchain,
    _id,
    f._inserted_timestamp
FROM
    FINAL f
    LEFT JOIN {{ ref('silver_ens__ens_domain_subdomains') }}
    s
    ON f.node = s.parent_node
GROUP BY
    ALL qualify(ROW_NUMBER() over (PARTITION BY label
ORDER BY
    last_registered_timestamp DESC)) = 1
