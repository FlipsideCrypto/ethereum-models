{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    cluster_by = ['slot_timestamp::date']
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
    {{ dbt_utils.surrogate_key(
        ['slot_number', 'INDEX']
    ) }} AS id
FROM
    {{ ref('silver__beacon_blocks') }},
    LATERAL FLATTEN(
        input => withdrawals
    )
WHERE
    epoch_number IS NOT NULL
    AND slot_number > 6209119 -- adjust this to the slot number of the first withdrawal

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
