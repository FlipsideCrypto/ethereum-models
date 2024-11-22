-- depends on: {{ ref('bronze__streamline_beacon_blocks') }}
{{ config(
    materialized = 'incremental',
    unique_key = 'slot_number',
    cluster_by = ['slot_timestamp::date'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(slot_number,parent_root,state_root,randao_reveal,graffiti,eth1_block_hash,eth1_deposit_root,signature,block_included)",
    incremental_predicates = ["dynamic_range", "slot_number"],
    full_refresh = false,
    tags = ['beacon']
) }}

SELECT
    COALESCE(
        VALUE :"SLOT_NUMBER" :: INT,
        metadata :request :"slot_number" :: INT,
        PARSE_JSON(
            metadata :request :"slot_number"
        ) :: INT
    ) AS slot_number,
    FLOOR(
        slot_number / 32
    ) AS epoch_number,
    TO_TIMESTAMP(
        DATA :message :body :execution_payload :timestamp :: INTEGER
    ) AS slot_timestamp,
    DATA :message :proposer_index :: INTEGER AS proposer_index,
    DATA :message :parent_root :: STRING AS parent_root,
    DATA :message :state_root :: STRING AS state_root,
    DATA :message :body :randao_reveal :: STRING AS randao_reveal,
    DATA :message :body :graffiti :: STRING AS graffiti,
    DATA :message :body :eth1_data :block_hash :: STRING AS eth1_block_hash,
    DATA :message :body :eth1_data :deposit_count :: INTEGER AS eth1_deposit_count,
    DATA :message :body :eth1_data :deposit_root :: STRING AS eth1_deposit_root,
    DATA :message :body :execution_payload AS execution_payload,
    DATA :signature :: STRING AS signature,
    DATA :message :body :attester_slashings AS attester_slashings,
    DATA :message :body :proposer_slashings AS proposer_slashings,
    DATA :message :body :deposits AS deposits,
    DATA :message :body :attestations AS attestations,
    DATA :message :body :execution_payload :withdrawals :: ARRAY AS withdrawals,
    OBJECT_DELETE(
        DATA :message :body,
        'attestations',
        DATA :message :body,
        'deposits',
        DATA :message :body :execution_payload,
        'withdrawals'
    ) AS slot_json,
    CASE
        WHEN DATA ILIKE '%not found%' THEN FALSE
        ELSE TRUE
    END AS block_included,
    _inserted_timestamp :: TIMESTAMP AS _inserted_timestamp,
    DATA,
    {{ dbt_utils.generate_surrogate_key(
        ['slot_number']
    ) }} AS beacon_blocks_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id,
    DATA :message :body :blob_kzg_commitments :: ARRAY AS blob_kzg_commitments,
    DATA :message :body :execution_payload :blob_gas_used :: INT AS blob_gas_used,
    DATA :message :body :execution_payload :excess_blob_gas :: INT AS excess_blob_gas
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_beacon_blocks') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
    AND ifnull(DATA,'') NOT ILIKE '%internal server error%'
{% else %}
    {{ ref('bronze__streamline_fr_beacon_blocks') }}
WHERE
    ifnull(DATA,'') NOT ILIKE '%internal server error%'
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY slot_number
ORDER BY
    _inserted_timestamp DESC)) = 1
