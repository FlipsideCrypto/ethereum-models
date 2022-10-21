{{ config(
    materialized = 'incremental',
    unique_key = 'slot_number',
    cluster_by = ['slot_timestamp::date'],
    merge_update_columns = ["id"]
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
    {{ dbt_utils.surrogate_key(
        ['slot_number', 'signature', 'proofs']
    ) }} AS id
FROM
    {{ ref('silver__beacon_blocks') }},
    LATERAL FLATTEN(
        input => deposits
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
