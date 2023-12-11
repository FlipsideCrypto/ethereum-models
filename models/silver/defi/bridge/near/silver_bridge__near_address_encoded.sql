{{ config(
    materialized = 'incremental',
    unique_key = "near_address",
    tags = ['curated']
) }}

WITH log_address AS (

    SELECT
        DISTINCT receiver_id AS near_address,
        CONCAT('0x', SHA2(near_address, 256)) AS addr_encoded
    FROM
        {{ source(
            'near_silver',
            'logs_s3'
        ) }}

{% if is_incremental() %}
WHERE
    near_address NOT IN (
        SELECT
            DISTINCT near_address
        FROM
            {{ this }}
    )
{% endif %}
UNION
SELECT
    DISTINCT signer_id AS near_address,
    CONCAT('0x', SHA2(near_address, 256)) AS addr_encoded
FROM
    {{ source(
        'near_silver',
        'logs_s3'
    ) }}

{% if is_incremental() %}
WHERE
    near_address NOT IN (
        SELECT
            DISTINCT near_address
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    near_address,
    addr_encoded
FROM
    log_address
