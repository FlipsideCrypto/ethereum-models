{{ config (
    materialized = 'view'
) }}

WITH base AS (

    SELECT
        _log_id,
        event_inputs :from :: STRING AS from_address,
        event_inputs :to :: STRING AS to_address,
        contract_address,
        block_number,
        block_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING IN (
            '0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65',
            '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        )
        AND (
            from_address IS NOT NULL
            OR to_address IS NOT NULL
        )
    EXCEPT
    SELECT
        _log_id
    FROM
        {{ source(
            'ethereum_external',
            ''
        ) }}
),
addresses AS (
    SELECT
        block_timestamp,
        block_number,
        contract_address,
        from_address AS address
    FROM
        base
    UNION ALL
    SELECT
        block_timestamp,
        block_number,
        contract_address,
        to_address AS address
    FROM
        base
)
SELECT
    DISTINCT block_timestamp :: DATE,
    contract_address,
    address
FROM
    addresses
WHERE
    address <> '0x0000000000000000000000000000000000000000'
