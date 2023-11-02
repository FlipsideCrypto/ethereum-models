{{ config (
    materialized = 'incremental',
    unique_key = ['contract_address','proxy_address'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
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
heal AS (
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
),
FINAL AS (
    SELECT
        contract_address,
        proxy_address,
        start_block,
        _id,
        _inserted_timestamp
    FROM
        heal qualify ROW_NUMBER() over (
            PARTITION BY contract_address,
            proxy_address
            ORDER BY
                start_block ASC
        ) = 1
)
SELECT
    f.contract_address,
    f.proxy_address,
    f.start_block,
    f._id,
    f._inserted_timestamp,
    C.block_number AS created_block,
    p.block_number AS proxy_created_block
FROM
    FINAL f
    JOIN {{ ref('silver__created_contracts') }} C
    ON f.contract_address = C.created_contract_address
    JOIN {{ ref('silver__created_contracts') }} p
    ON f.proxy_address = p.created_contract_address