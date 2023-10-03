{{ config(
    materialized = 'incremental',
    unique_key = 'collection_page',
    tags = ['nft_metadata']
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
{% endif %}
LIMIT
    100
), numbered AS (
    SELECT
        *,
        ROW_NUMBER() over (
            ORDER BY
                nft_address,
                current_page ASC
        ) AS row_num
    FROM
        raw
),
requests AS ({% for item in range(5) %}
    (
SELECT
    nft_address, current_page, end_page, collection_page, row_num, ethereum.streamline.udf_api('POST', node_url,{}, PARSE_JSON(json_request)) AS api_resp, SYSDATE() AS _inserted_timestamp
FROM
    numbered

{% if is_incremental() %}
WHERE
    row_num BETWEEN ({{ item }} * 20 + 1)
    AND ((({{ item }} + 1) * 20))
{% else %}
WHERE
    row_num BETWEEN ({{ item }} * 20 + 1)
    AND ((({{ item }} + 1) * 20))
{% endif %}) {% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %})
SELECT
    *
FROM
    requests
