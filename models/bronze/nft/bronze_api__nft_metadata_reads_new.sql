{{ config(
    materialized = 'incremental',
    unique_key = 'collection_page',
    tags = ['stale'],
    full_refresh = false
) }}

WITH raw AS (

    SELECT
        *
    FROM
        {{ ref('bronze_api__nft_metadata_list_request') }}

{% if is_incremental() %}
WHERE
    collection_page NOT IN (
        SELECT
            collection_page
        FROM
            {{ this }}
    )
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
        ) <= 100
),
requests AS ({% for item in range(10) %}
    (
SELECT
    nft_address, current_page, end_page, collection_page, row_num, live.udf_api('POST', CONCAT('{service}', '/', '{Authentication}'),{}, json_request, 'Vault/prod/ethereum/quicknode/mainnet') AS api_resp, SYSDATE() AS _inserted_timestamp
FROM
    numbered

{% if is_incremental() %}
WHERE
    row_num BETWEEN ({{ item }} * 10 + 1)
    AND ((({{ item }} + 1) * 10))
{% else %}
WHERE
    row_num BETWEEN ({{ item }} * 10 + 1)
    AND ((({{ item }} + 1) * 10))
{% endif %}) {% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %})
SELECT
    *
FROM
    requests
