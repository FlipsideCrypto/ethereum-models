{{ config (
    materialized = 'incremental',
    unique_key = ['contract_address','proxy_address'],
    tags = ['abis']
) }}

WITH base AS (

    SELECT
        from_address,
        to_address,
        MIN(block_number) AS start_block,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM
        {{ ref('silver__traces') }}
    WHERE
        TYPE = 'DELEGATECALL'
        AND trace_status = 'SUCCESS'
        AND tx_status = 'SUCCESS'
        AND from_address != to_address -- exclude self-calls

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '24 hours'
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    from_address,
    to_address
),
create_id AS (
    SELECT
        from_address AS contract_address,
        to_address AS proxy_address,
        start_block,
        CONCAT(
            from_address,
            '-',
            to_address
        ) AS _id,
        _inserted_timestamp
    FROM
        base
),
FINAL AS (
    SELECT
        contract_address,
        proxy_address,
        start_block,
        _id,
        _inserted_timestamp
    FROM
        create_id

{% if is_incremental() %}
UNION ALL
SELECT
    contract_address,
    proxy_address,
    start_block,
    _id,
    _inserted_timestamp
FROM
    {{ this }}
    JOIN create_id USING (
        contract_address,
        proxy_address
    )
{% endif %}
)
SELECT
    contract_address,
    proxy_address,
    start_block,
    _id,
    _inserted_timestamp
FROM
    FINAL qualify ROW_NUMBER() over (
        PARTITION BY contract_address,
        proxy_address
        ORDER BY
            start_block ASC
    ) = 1
