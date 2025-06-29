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
        topics [0] :: STRING = '0x8ce7013e8abebc55c3890a68f5a27c67c3f7efa64e584de5fb22363c606fd340'
        AND contract_address = '0xd4416b13d2b3a9abae7acd5d6c2bbdbe25686401'

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
        SUBSTRING(
            decoded_flat :"name" :: STRING,
            3
        ) AS name_raw,
        COALESCE(TRY_HEX_DECODE_STRING(name_raw), name_raw) AS full_name,
        COALESCE(utils.udf_hex_to_string(name_raw), name_raw) AS name_unadj,
        decoded_flat :"node" :: STRING AS node,
        decoded_flat :"owner" :: STRING AS owner,
        _log_id,
        _inserted_timestamp
    FROM
        base_events),
        generate_rows AS (
            SELECT
                ROW_NUMBER() over(
                    ORDER BY
                        SEQ4()
                ) AS idx
            FROM
                TABLE(GENERATOR(rowcount => 1000))
        ),
        transform_name AS (
            SELECT
                *,
                COALESCE(
                    CASE
                        WHEN ASCII(SUBSTRING(w.full_name, r.idx, 1)) < 32 THEN '.'
                        ELSE SUBSTRING(
                            w.full_name,
                            r.idx,
                            1
                        )
                    END,
                    ''
                ) AS char_val
            FROM
                WRAPPED w
                LEFT JOIN generate_rows r
                ON r.idx <= LENGTH(
                    w.full_name
                )
        ),
        grouped_name AS (
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
                name_raw,
                full_name,
                TRIM(LISTAGG(char_val, '') within GROUP (
            ORDER BY
                idx), '.') AS processed_name,
                name_unadj,
                node,
                owner,
                expiry,
                expiry_timestamp,
                fuses,
                _log_id,
                _inserted_timestamp
            FROM
                transform_name
            GROUP BY
                ALL
        ),
        split_name AS (
            SELECT
                *,
                SPLIT(
                    processed_name,
                    '.'
                ) AS name_parts
            FROM
                grouped_name
        ),
        name_obj AS (
            SELECT
                *,
                ARRAY_SIZE(name_parts) AS num,
                CASE
                    WHEN num > 2 THEN ARRAY_SLICE(
                        name_parts,
                        0,
                        num -2
                    )
                    ELSE NULL
                END AS subdomains,
                name_parts [num-2] AS domain,
                name_parts [num-1] AS tld,
                OBJECT_CONSTRUCT(
                    'subdomains',
                    subdomains,
                    'parent',
                    domain,
                    'tld',
                    tld
                ) AS domain_obj
            FROM
                split_name
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
        processed_name AS ens_domain,
        domain_obj AS ens_domain_obj,
        node,
        utils.udf_keccak256(
            name_parts [0] :: STRING
        ) AS label,
        CASE
            WHEN ARRAY_SIZE(name_parts) > 2 THEN utils.udf_keccak256(
                name_parts [ARRAY_SIZE(name_parts)-2] :: STRING
            )
            ELSE NULL
        END AS parent_label,
        origin_from_address AS manager,
        owner,
        expiry,
        expiry_timestamp,
        fuses,
        _log_id,
        _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
             ['tx_hash','event_index']
        ) }} AS ens_domain_wrapped_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        name_obj
