{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
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
            '0x8ce7013e8abebc55c3890a68f5a27c67c3f7efa64e584de5fb22363c606fd340',
            '0xee2ba1195c65bcf218a83d874335c6bf9d9067b4c672f3c3bf16cf40de7586c4'
        )
        AND contract_address = '0xd4416b13d2b3a9abae7acd5d6c2bbdbe25686401'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
),
WRAPPED AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        decoded_flat :"expiry" :: STRING AS expiry,
        TRY_TO_TIMESTAMP(expiry) AS expiry_timestamp,
        TRY_TO_NUMBER(
            decoded_flat :"fuses" :: STRING
        ) AS fuses,
        decoded_flat :"name" :: STRING AS name_raw,
        utils.udf_hex_to_string(SUBSTRING(name_raw, 3)) AS NAME,
        decoded_flat :"node" :: STRING AS node,
        decoded_flat :"owner" :: STRING AS owner,
        _log_id,
        _inserted_timestamp
    FROM
        base_events
    WHERE
        topic_0 = '0x8ce7013e8abebc55c3890a68f5a27c67c3f7efa64e584de5fb22363c606fd340'
),
unwrapped AS (
    SELECT
        u.block_number,
        u.block_timestamp,
        u.tx_hash,
        u.origin_function_signature,
        u.origin_from_address,
        u.origin_to_address,
        u.contract_address,
        u.event_index,
        u.event_name,
        u.decoded_flat :"node" :: STRING AS node,
        u.decoded_flat :"owner" :: STRING AS owner,
        name_raw,
        NAME,
        u._log_id,
        u._inserted_timestamp
    FROM
        base_events u
        LEFT JOIN WRAPPED w
        ON u.decoded_flat :"owner" :: STRING = w.owner
        AND u.decoded_flat :"node" :: STRING = w.node
    WHERE
        topic_0 = '0xee2ba1195c65bcf218a83d874335c6bf9d9067b4c672f3c3bf16cf40de7586c4'
),
FINAL AS (
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
        node,
        owner,
        expiry,
        expiry_timestamp,
        fuses,
        _log_id,
        _inserted_timestamp
    FROM
        WRAPPED
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
        NAME,
        node,
        owner,
        NULL AS expiry,
        NULL AS expiry_timestamp,
        NULL AS fuses,
        _log_id,
        _inserted_timestamp
    FROM
        unwrapped
)
SELECT
    f.block_number,
    f.block_timestamp,
    f.tx_hash,
    f.origin_function_signature,
    f.origin_from_address,
    f.origin_to_address,
    f.contract_address,
    f.event_index,
    f.event_name,
    NAME,
    node,
    owner,
    OPERATOR,
    token_id,
    expiry,
    expiry_timestamp,
    fuses,
    f._log_id,
    f._inserted_timestamp
FROM
    FINAL f
    LEFT JOIN {{ ref('silver_ens__ens_domain_transfers') }}
    t
    ON f.block_number = t.block_number
    AND f.tx_hash = t.tx_hash 
    qualify(ROW_NUMBER() over (PARTITION BY f._log_id
ORDER BY
    f._inserted_timestamp DESC, operator nulls last)) = 1
