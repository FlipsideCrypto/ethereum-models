{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
) }}

WITH base AS (

    SELECT
        CONCAT('0x', SUBSTR(l.topics [1] :: STRING, 27, 42)) AS address1,
        CONCAT('0x', SUBSTR(l.topics [2] :: STRING, 27, 42)) AS address2,
        l.contract_address,
        l.block_number,
        l.block_timestamp :: DATE AS _block_date,
        TO_TIMESTAMP_NTZ(
            l._inserted_timestamp
        ) AS _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
    WHERE
        (
            l.topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
            OR (
                l.topics [0] :: STRING = '0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65'
                AND l.contract_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            )
            OR (
                l.topics [0] :: STRING = '0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c'
                AND l.contract_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            )
        ) -- /*
        -- * 0xddf252ad = token transfer
        -- * 0x7fcf532c = withdrawl (WETH)
        -- * 0xe1fffcc4 = deposit (WETH)
        -- */

{% if is_incremental() %}
AND l._inserted_timestamp >= COALESCE(
    (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    ),
    '1900-01-01'
)
{% endif %}
),
transfers AS (
    SELECT
        DISTINCT block_number,
        contract_address,
        address1 AS address,
        _inserted_timestamp
    FROM
        base
    WHERE
        address1 IS NOT NULL
        AND address1 <> '0x0000000000000000000000000000000000000000'
    UNION
    SELECT
        DISTINCT block_number,
        contract_address,
        address2 AS address,
        _inserted_timestamp
    FROM
        base
    WHERE
        address2 IS NOT NULL
        AND address2 <> '0x0000000000000000000000000000000000000000'
),
pending AS (
    SELECT
        t.block_number,
        t.address,
        t.contract_address,
        t._inserted_timestamp
    FROM
        transfers t
)
SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['block_number', 'contract_address', 'address']
    ) }} AS id,
    block_number,
    address,
    contract_address,
    _inserted_timestamp
FROM
    pending
