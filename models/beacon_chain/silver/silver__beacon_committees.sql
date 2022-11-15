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
        func_type = 'committees'

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
    VALUE :index :: STRING AS INDEX,
    VALUE :slot :: STRING AS slot,
    VALUE :validators AS validators,
    {{ dbt_utils.surrogate_key(
        ['block_number', 'INDEX']
    ) }} AS id
FROM
    base,
    LATERAL FLATTEN (
        input => DATA
    )
