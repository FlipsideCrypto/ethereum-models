{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    cluster_by = ['slot_timestamp::date'],
    merge_update_columns = ["id"]
) }}

SELECT
    slot_number,
    slot_timestamp,
    epoch_number,
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
        ['slot_number', 'attestation_index', 'aggregation_bits', 'beacon_block_root', 'attestation_signature']
    ) }} AS id
FROM
    {{ ref('silver__beacon_blocks') }},
    LATERAL FLATTEN(
        input => attestations
    )

{% if is_incremental() %}
WHERE
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
