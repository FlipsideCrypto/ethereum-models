-- depends on: {{ ref('bronze__beacon_blocks') }}
{{ config(
    materialized = 'incremental',
    unique_key = 'slot_number',
    cluster_by = ['slot_timestamp::date'],
    on_schema_change = 'append_new_columns',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(slot_number)",
    incremental_predicates = ["dynamic_range", "slot_number"],
    full_refresh = false
) }}

SELECT
    slot_number,
    DATA :message :body :attestations [0] :data :target :epoch :: INTEGER AS epoch_number,
    -- status
    TO_TIMESTAMP(
        DATA :message :body :execution_payload :timestamp :: INTEGER
    ) AS slot_timestamp,
    DATA :message :proposer_index :: INTEGER AS proposer_index,
    -- will need to link to other source for validator via the index above
    -- blockRoot Hash
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
    _inserted_timestamp :: TIMESTAMP AS _inserted_timestamp,
    DATA
FROM

{% if is_incremental() %}
{{ ref('bronze__beacon_blocks') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__fr_beacon_blocks') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY slot_number
ORDER BY
    _inserted_timestamp DESC)) = 1
