{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"]
) }}

WITH base_data AS (

    SELECT
        block_number,
        block_timestamp :: DATE AS _block_date,
        from_address,
        to_address,
        _inserted_timestamp
    FROM
        {{ ref('silver__traces') }}
    WHERE
        eth_value > 0

{% if is_incremental() %}
AND (
    _inserted_timestamp >= COALESCE(
        (
            SELECT
                MAX(_inserted_timestamp)
            FROM
                {{ this }}
        ),
        '1900-01-01'
    )
)
{% endif %}
),
stacked AS (
    SELECT
        DISTINCT block_number,
        from_address AS address,
        _inserted_timestamp
    FROM
        base_data
    WHERE
        from_address IS NOT NULL
        AND from_address <> '0x0000000000000000000000000000000000000000'
    UNION
    SELECT
        DISTINCT block_number,
        to_address AS address,
        _inserted_timestamp
    FROM
        base_data
    WHERE
        to_address IS NOT NULL
        AND to_address <> '0x0000000000000000000000000000000000000000'
),
pending AS (
    SELECT
        s.block_number,
        s.address,
        s._inserted_timestamp
    FROM
        stacked s
)
SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['block_number', 'address']
    ) }} AS id,
    block_number,
    address,
    _inserted_timestamp
FROM
    pending
