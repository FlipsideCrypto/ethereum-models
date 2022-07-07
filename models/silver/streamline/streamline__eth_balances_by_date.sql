{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"]
) }}

WITH block_date AS (

    SELECT
        block_date,
        block_number
    FROM
        {{ ref("_max_block_by_date") }}
),
base_data AS (
    SELECT
        block_number,
        block_timestamp :: DATE AS _block_date,
        from_address,
        to_address,
        ingested_at AS _ingested_at
    FROM
        {{ ref('silver__traces') }}
    WHERE
        eth_value > 0

{% if is_incremental() %}
AND (
    ingested_at >= COALESCE(
        (
            SELECT
                MAX(_inserted_timestamp)
            FROM
                {{ this }}
        ),
        '1900-01-01'
    )
    OR block_number IN (
        -- /*
        -- * If the block is not in the database, we need to ingest it.
        -- * This is to handle the case where the block is not in the database
        -- * because it was not loaded into the database.
        -- */
        SELECT
            block_number
        FROM
            {{ this }}
        WHERE
            block_number IS NULL
    )
)
{% endif %}
),
stacked AS (
    SELECT
        DISTINCT _block_date,
        from_address AS address
    FROM
        base_data
    WHERE
        from_address IS NOT NULL
        AND from_address <> '0x0000000000000000000000000000000000000000'
    UNION
    SELECT
        DISTINCT _block_date,
        to_address AS address
    FROM
        base_data
    WHERE
        to_address IS NOT NULL
        AND to_address <> '0x0000000000000000000000000000000000000000'
),
pending AS (
    SELECT
        b.block_number,
        s.address
    FROM
        stacked s
        LEFT JOIN block_date b
        ON s._block_date = b._block_date
)
SELECT
    {{ dbt_utils.surrogate_key(
        ['block_number', 'address']
    ) }} AS id,
    block_number,
    address,
    SYSDATE() AS _inserted_timestamp
FROM
    pending
