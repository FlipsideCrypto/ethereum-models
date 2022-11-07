{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    cluster_by = ['block_number'],
    merge_update_columns = ["id"]
) }}

WITH base AS (

    SELECT
        *
    FROM
        {{ ref('silver__beacon_all_validators') }}
    WHERE
        func_type = 'validators'

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
)
SELECT
    block_number,
    state_id,
    INDEX,
    DATA :balance :: INTEGER / pow(
        10,
        9
    ) AS balance,
    DATA :status :: STRING AS validator_status,
    DATA :validator :activation_eligibility_epoch :: INTEGER AS activation_eligibility_epoch,
    DATA :validator :activation_epoch :: INTEGER AS activation_epoch,
    DATA :validator: effective_balance :: INTEGER / pow(
        10,
        9
    ) AS effective_balance,
    DATA :validator: exit_epoch :: INTEGER AS exit_epoch,
    DATA :validator: pubkey :: STRING AS pubkey,
    DATA :validator: slashed :: BOOLEAN AS slashed,
    DATA :validator: withdrawable_epoch :: INTEGER AS withdrawable_epoch,
    DATA :validator: withdrawal_credentials :: STRING AS withdrawal_credentials,
    DATA :validator AS validator_details,
    _inserted_timestamp,
    {{ dbt_utils.surrogate_key(
        ['block_number', 'index']
    ) }} AS id
FROM
    base qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
