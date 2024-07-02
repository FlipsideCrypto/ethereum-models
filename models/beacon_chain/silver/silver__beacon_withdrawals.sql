{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    cluster_by = ['slot_timestamp::date'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(address), SUBSTRING(address)",
    tags = ['beacon']
) }}

SELECT
    slot_number,
    slot_timestamp,
    epoch_number,
    VALUE :amount :: INTEGER AS withdrawal_amount,
    VALUE :address :: STRING AS address,
    VALUE :index :: INTEGER AS INDEX,
    VALUE :validator_index :: INTEGER AS validator_index,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['slot_number', 'INDEX']
    ) }} AS id,
    id AS beacon_withdrawals_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__beacon_blocks') }},
    LATERAL FLATTEN(
        input => withdrawals
    )
WHERE
    withdrawals IS NOT NULL
    AND slot_number > 6209119 -- slot number of the first withdrawal

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        COALESCE(MAX(_inserted_timestamp), '1970-01-01' :: DATE)
    FROM
        {{ this }})
    {% endif %}

    qualify(ROW_NUMBER() over (PARTITION BY id
    ORDER BY
        _inserted_timestamp DESC)) = 1
