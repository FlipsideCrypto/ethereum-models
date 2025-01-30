-- depends_on: {{ ref('bronze__streamline_beacon_blobs') }}
{{ config(
    materialized = 'incremental',
    unique_key = 'blob_sidecar_id',
    cluster_by = "ROUND(slot_number, -3)",
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(blob,kzg_commitment,kzg_proof,body_root,parent_root,state_root,signature)",
    tags = ['beacon']
) }}

WITH old_base AS (

    SELECT
        slot_number,
        resp,
        _inserted_timestamp
    FROM
        {{ ref("bronze__blob_sidecars") }}

{% if is_incremental() %}
WHERE
    1 = 2
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
        old_base,
        LATERAL FLATTEN (
            input => resp :data :data
        )
), 
new_data as (
    SELECT
        DATA :blob :: STRING AS blob,
        array_index :: INT AS blob_index,
        DATA :kzg_commitment :: STRING AS kzg_commitment,
        DATA :kzg_commitment_inclusion_proof :: ARRAY AS kzg_commitment_inclusion_proof,
        DATA :kzg_proof :: STRING AS kzg_proof,
        DATA :signed_block_header :message :body_root :: STRING AS body_root,
        DATA :signed_block_header :message :parent_root :: STRING AS parent_root,
        DATA :signed_block_header :message :proposer_index :: INT AS proposer_index,
        DATA :signed_block_header :message :slot :: INT AS slot_number,
        DATA :signed_block_header :message :state_root :: STRING AS state_root,
        DATA :signed_block_header :signature :: STRING AS signature,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_beacon_blobs') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
    AND (LEFT(DATA :error :: STRING, 1) <> 'F'
    OR DATA :error IS NULL)
    and not is_array(DATA)
{% else %}
    {{ ref('bronze__streamline_fr_beacon_blobs') }}
WHERE
    (LEFT(
        DATA :error :: STRING,
        1
    ) <> 'F'
    OR DATA :error IS NULL)
    and not is_array(DATA)
{% endif %}
), 
old_and_new_data as (
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
    _inserted_timestamp
FROM
    flat_response
UNION ALL
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
    _inserted_timestamp
FROM new_data
)
select 
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
    ) }} AS blob_sidecar_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
from old_and_new_data

qualify(ROW_NUMBER() over (PARTITION BY slot_number, blob_index
ORDER BY
    _inserted_timestamp DESC)) = 1