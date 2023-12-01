{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    cluster_by = ['slot_timestamp::date'],
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['beacon']
) }}

SELECT
    slot_number,
    slot_timestamp,
    epoch_number,
    VALUE :data :amount :: INTEGER AS deposit_amount,
    VALUE :data :pubkey :: STRING AS pubkey,
    VALUE :data :signature :: STRING AS signature,
    VALUE :data :withdrawal_credentials :: STRING AS withdrawal_credentials,
    VALUE :proof AS proofs,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['slot_number', 'signature', 'proofs']
    ) }} AS id,
    id AS beacon_deposits_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__beacon_blocks') }},
    LATERAL FLATTEN(
        input => deposits
    )
WHERE
    deposits IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp >= (
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
