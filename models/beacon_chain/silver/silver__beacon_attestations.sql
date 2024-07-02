{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    cluster_by = ['slot_timestamp::date'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(aggregation_bits,beacon_block_root,source_root,target_root,attestation_signature),SUBSTRING(aggregation_bits,beacon_block_root,source_root,target_root,attestation_signature)",
    tags = ['beacon']
) }}

SELECT
    slot_number,
    slot_timestamp,
    epoch_number,
    VALUE :data :slot :: INTEGER AS attestation_slot,
    VALUE :data :index :: INTEGER AS attestation_index,
    VALUE :aggregation_bits :: STRING AS aggregation_bits,
    VALUE :data :beacon_block_root :: STRING AS beacon_block_root,
    VALUE :data :source :epoch :: INTEGER AS source_epoch,
    VALUE :data :source :root :: STRING AS source_root,
    VALUE :data :target :epoch :: INTEGER AS target_epoch,
    VALUE :data :target :root :: STRING AS target_root,
    VALUE :signature :: STRING AS attestation_signature,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['slot_number', 'attestation_slot', 'attestation_index', 'aggregation_bits', 'beacon_block_root', 'attestation_signature']
    ) }} AS id,
    id as beacon_attestation_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__beacon_blocks') }},
    LATERAL FLATTEN(
        input => attestations
    )
WHERE attestations IS NOT NULL

{% if is_incremental() %}
AND
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
