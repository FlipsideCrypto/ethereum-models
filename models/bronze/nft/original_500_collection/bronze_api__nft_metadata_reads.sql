{{ config(
    materialized = 'incremental',
    unique_key = 'collection_page',
    tags = ['bronze','nft','nft_metadata']
) }}

WITH raw AS (

    SELECT
        *
    FROM
        {{ ref('bronze_api__nft_metadata_list') }}

{% if is_incremental() %}
WHERE
    collection_page NOT IN (
        SELECT
            collection_page
        FROM
            {{ this }}
    )
    AND nft_address != '0x60f80121c31a0d46b5279700f9df786054aa5ee5'
    AND collection_page NOT IN ('0xd07dc4262bcdbf85190c01c996b4c06a461d2430-1740') -- rarible 3 sunflowers
{% endif %}
),
numbered AS (
    SELECT
        *,
        ROW_NUMBER() over (
            ORDER BY
                collection_page ASC
        ) AS row_num
    FROM
        raw qualify ROW_NUMBER() over (
            ORDER BY
                collection_page ASC
        ) <= 50
),
requests AS ({% for item in range(10) %}
    (
SELECT
    nft_address, current_page, end_page, collection_page, row_num, live.udf_api('POST', CONCAT('{service}', '/', '{Authentication}'),{}, json_request, 'Vault/prod/ethereum/quicknode/mainnet') AS resp, SYSDATE() AS _inserted_timestamp
FROM
    numbered

{% if is_incremental() %}
WHERE
    row_num BETWEEN ({{ item }} * 5 + 1)
    AND ((({{ item }} + 1) * 5))
{% else %}
WHERE
    row_num BETWEEN ({{ item }} * 5 + 1)
    AND ((({{ item }} + 1) * 5))
{% endif %}) {% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %})
SELECT
    *
FROM
    requests
