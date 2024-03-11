{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    full_refresh = false,
    tags = ['beacon']
) }}

WITH current_slot AS (

    SELECT
        slot_number AS max_slot
    FROM
        {{ ref("bronze__sepolia_slots") }}
    WHERE
        inserted_timestamp = (
            SELECT
                MAX(inserted_timestamp)
            FROM
                {{ ref("bronze__sepolia_slots") }}
        )
),
create_range AS (
    SELECT
        _id AS slot,
        ROW_NUMBER() over (
            ORDER BY
                _id
        ) AS row_no,
        CEIL(
            row_no / 10
        ) AS batch_no
    FROM
        {{ ref("silver__number_sequence") }}
        JOIN current_slot
        ON max_slot >= _id
    WHERE
        _id >= 4536925 -- staring point, can be changed

{% if is_incremental() %}
AND _id NOT IN (
    SELECT
        DISTINCT slot_number
    FROM
        {{ this }}
)
{% endif %}
),
make_request AS ({% for item in range(800) %}
SELECT
    live.udf_api('GET', CONCAT('{Service}', '/', '{Authentication}', 'eth/v1/beacon/blob_sidecars/', slot :: STRING),{},{}, 'Vault/prod/ethereum/quicknode/sepolia') AS resp
FROM
    create_range
WHERE
    batch_no = {{ item }} + 1 {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}),
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
        VALUE :signed_block_header :signature :: STRING AS signature
    FROM
        make_request,
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
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id,
    {{ dbt_utils.generate_surrogate_key(
        ['slot_number', 'blob_index']
    ) }} AS id
FROM
    flat_response
