{{ config(
    materialized = 'incremental',
    unique_key = 'collection_page'
) }}

WITH batched AS ({% for item in range(15) %}

    SELECT
        nft_address, current_page, end_page, collection_page, row_num, ethereum.streamline.udf_api('POST', node_url,{}, PARSE_JSON(json_request)) AS api_resp, SYSDATE() AS _inserted_timestamp
    FROM
        {{ ref('bronze_api__nft_metadata_list') }}
    WHERE
        {# row_num BETWEEN {{ item * 10 + 1 }}
        AND {{(item + 1) * 10 }}
        #}
        batch_num = {{ item }} + 1

{% if is_incremental() %}
AND collection_page NOT IN (
SELECT
    collection_page
FROM
    {{ this }})
{% else %}
{% endif %}
AND EXISTS (
SELECT
    1
FROM
    {{ ref('bronze_api__nft_metadata_list') }}
WHERE
    {# row_num BETWEEN {{ item * 10 + 1 }}
    AND {{(item + 1) * 10 }}
    #}
    batch_num = {{ item }} + 1
LIMIT
    1) {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %})
SELECT
    nft_address,
    current_page,
    end_page,
    collection_page,
    row_num,
    api_resp,
    _inserted_timestamp
FROM
    batched
