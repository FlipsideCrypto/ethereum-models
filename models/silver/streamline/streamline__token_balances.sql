{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
) }}

WITH block_date AS (

    SELECT
        block_timestamp :: DATE AS block_date,
        MAX(block_number) block_number,
    FROM
        {{ ref("core__fact_blocks") }}
    GROUP BY
        block_timestamp :: DATE
),
base AS (
    SELECT
        l.event_inputs :from :: STRING AS from_address,
        l.event_inputs :to :: STRING AS to_address,
        l.contract_address,
        l.block_number,
        l.block_timestamp :: DATE AS _block_date,
        l.ingested_at AS _ingested_at
    FROM
        {{ ref('silver__logs') }}
        l
    WHERE
        l.topics [0] :: STRING IN (
            '0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65',
            '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        )
        AND (
            from_address IS NOT NULL
            OR to_address IS NOT NULL
        )
        AND (
            from_address <> '0x0000000000000000000000000000000000000000'
            OR to_address <> '0x0000000000000000000000000000000000000000'
        )

{% if is_incremental() %}
AND (
    l.ingested_at >= COALESCE(
        (
            SELECT
                MAX(_inserted_timestamp)
            FROM
                {{ this }}
        ),
        '1900-01-01'
    ) - INTERVAL '1 hour' -- remove when _inserted_timestamp is added to silver__logs
    OR l.block_number IN (
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
        contract_address,
        from_address AS address
    FROM
        base
    UNION
    SELECT
        DISTINCT _block_date,
        contract_address,
        to_address AS address
    FROM
        base
)
SELECT
    {{ dbt_utils.surrogate_key(
        ['block_number', 'contract_address', 'address']
    ) }} AS id,
    b.block_number,
    s.address,
    s.contract_address,
    SYSDATE() AS _inserted_timestamp
FROM
    stacked s
    INNER JOIN block_date b
    ON b.block_date = s._block_date
