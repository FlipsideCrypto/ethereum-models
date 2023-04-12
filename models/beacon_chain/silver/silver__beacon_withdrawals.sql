{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    cluster_by = ['slot_timestamp::date'],
    merge_update_columns = ["id"]
) }}
-- change columns once we have a better understanding of the data

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
        input => withdrawals
    )
WHERE
    epoch_number IS NOT NULL

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
