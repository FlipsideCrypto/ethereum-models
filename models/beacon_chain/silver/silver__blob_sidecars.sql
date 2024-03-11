{{ config(
    materialized = 'incremental',
    unique_key = 'blob_sidecar_id',
    cluster_by = "ROUND(slot_number, -3)",
    tags = ['beacon']
) }}

WITH base AS (

    SELECT
        slot_number,
        resp,
        _inserted_timestamp
    FROM
        {{ ref("bronze__blob_sidecars") }}

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
),
flat_response AS (
    SELECT
        VALUE :blob :: STRING AS blob,
        VALUE :index :: INT AS blob_index,
        VALUE :kzg_commitment :: STRING AS kzg_commitment,
        VALUE :kzg_commitment_inclusion_proof :: ARRAY AS kzg_commitment_inclusion_proof,
        VALUE :kzg_proof :: STRING AS kzg_proof,
        VALUE :signed_block_header :message :body_root :: STRING AS body_root,
        VALUE :signed_block_header :message :parent_root :: STRING AS parent_root,
        VALUE :signed_block_header :message :proposer_index :: INT AS proposer_index,
        VALUE :signed_block_header :message :slot :: INT AS slot_number,
        VALUE :signed_block_header :message :state_root :: STRING AS state_root,
        VALUE :signed_block_header :signature :: STRING AS signature,
        _INSERTED_TIMESTAMP
    FROM
        base,
        LATERAL FLATTEN (
            input => resp :data :data
        )
)
SELECT
    blob,
    blob_index,
    kzg_commitment,
    kzg_commitment_inclusion_proof,
    kzg_proof,
    body_root,
    parent_root,
    proposer_index,
    slot_number,
    state_root,
    signature,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id,
    {{ dbt_utils.generate_surrogate_key(
        ['slot_number', 'blob_index']
    ) }} AS blob_sidecar_id
FROM
    flat_response
