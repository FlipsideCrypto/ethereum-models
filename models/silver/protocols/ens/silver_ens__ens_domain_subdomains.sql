{{ config(
    materialized = 'incremental',
    unique_key = 'tx_hash',
    tags = ['non_realtime']
) }}

WITH base AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        from_address,
        to_address,
        input_data,
        regexp_substr_all(SUBSTR(input_data, 11, len(input_data)), '.{64}') AS segmented_data,
        CONCAT('0x',segmented_data[0]::STRING) AS parent_node,
        CONCAT('0x',SUBSTR(segmented_data[2]::STRING,25,40)) AS new_owner,
        CONCAT('0x',SUBSTR(segmented_data[3]::STRING,25,40)) AS resolver,
        utils.udf_hex_to_string(
            segmented_data[8] :: STRING
        ) AS ens_subdomain,
        tx_status,
        _inserted_timestamp
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        tx_status = 'SUCCESS'
        AND origin_function_signature = '0x24c1af44'
        AND to_address = '0xd4416b13d2b3a9abae7acd5d6c2bbdbe25686401'
{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    from_address,
    to_address,
    input_data,
    parent_node,
    new_owner,
    resolver,
    ens_subdomain,
    tx_status,
    _inserted_timestamp
FROM
    base