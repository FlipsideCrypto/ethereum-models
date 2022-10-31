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
        func_type = 'validator_balances'

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
    _inserted_timestamp,
    {{ dbt_utils.surrogate_key(
        ['block_number', 'index']
    ) }} AS id
FROM
    base qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
