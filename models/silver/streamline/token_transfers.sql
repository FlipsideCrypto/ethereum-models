{{ config (
    materialized = "ephemeral",
    unique_key = ["contract_address", "address"]
) }}

WITH base AS (

    SELECT
        CONCAT('0x', SUBSTR(l.topics [1] :: STRING, 27, 42)) AS address1,
        CONCAT('0x', SUBSTR(l.topics [2] :: STRING, 27, 42)) AS address2,
        l.contract_address,
        l.block_number,
        l.block_timestamp :: DATE AS _block_date,
        l.ingested_at AS _ingested_at
    FROM
        {{ ref('silver__logs') }}
        l
    WHERE
        l.topics [0] :: STRING IN (
            '0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c',
            '0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65',
            '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        ) -- /*
        -- * 0xddf252ad = token transfer
        -- * 0x7fcf532c = withdrawl (WETH)
        -- * 0xe1fffcc4 = deposit (WETH)
        -- */

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
    ) - INTERVAL '1 hour' -- TODO: remove when _inserted_timestamp is added to silver__logs
    OR l.block_number IN (
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
)
SELECT
    DISTINCT _block_date,
    contract_address,
    address1 AS address
FROM
    base
WHERE
    address1 IS NOT NULL
    AND address1 <> '0x0000000000000000000000000000000000000000'
UNION
SELECT
    DISTINCT _block_date,
    contract_address,
    address2 AS address
FROM
    base
WHERE
    address2 IS NOT NULL
    AND address2 <> '0x0000000000000000000000000000000000000000'
