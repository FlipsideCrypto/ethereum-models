{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"]
) }}

WITH base AS (

    SELECT
        *
    FROM
        {{ ref('silver__beacon_all_committees') }}
    WHERE
        func_type = 'sync_committees'

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
    DATA :validator_aggregates AS validator_aggregates,
    DATA :validators AS validators,
    _inserted_timestamp,
    {{ dbt_utils.surrogate_key(
        ['block_number']
    ) }} AS id
FROM
    base