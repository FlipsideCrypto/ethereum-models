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
    slot_timestamp,
    state_id,
    _inserted_timestamp,
    DATA :validator_aggregates AS validator_aggregates,
    DATA: validators AS validators,
    {{ dbt_utils.surrogate_key(
        ['block_number']
    ) }} AS id
FROM
    base
