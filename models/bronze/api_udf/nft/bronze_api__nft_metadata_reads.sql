{{ config(
    materialized = 'incremental',
    unique_key = 'collection_page'
) }}

WITH ready_requests AS (

    SELECT
        nft_address,
        current_page,
        end_page,
        collection_page,
        CONCAT(
            '{\'id\': 67, \'jsonrpc\': \'2.0\', \'method\': \'',
            method,
            '\',\'params\': [{ \'collection\': \'',
            nft_address,
            '\', \'page\': ',
            current_page,
            ',\'perPage\': 100 } ]}'
        ) AS json_request,
        node_url,
        ROW_NUMBER() over (
            ORDER BY
                nft_address
        ) AS row_no,
        FLOOR(
            row_no / 3
        ) + 1 AS batch_no
    FROM
        {{ ref('bronze_api__top_nft_list') }}
        JOIN {{ source(
            'streamline_crosschain',
            'node_mapping'
        ) }}
        ON 1 = 1
    WHERE
        chain = 'ethereum'

{% if is_incremental() %}
AND collection_page NOT IN (
    SELECT
        collection_page
    FROM
        {{ this }}
)
{% endif %}
),
batched AS ({% for item in range(15) %}
SELECT
    nft_address, current_page, end_page, collection_page, ethereum.streamline.udf_api('POST', node_url,{}, PARSE_JSON(json_request)) AS api_resp, SYSDATE() AS _inserted_timestamp
FROM
    ready_requests
WHERE
    batch_no = {{ item }} + 1
    AND EXISTS (
SELECT
    1
FROM
    ready_requests
WHERE
    batch_no = {{ item }} + 1
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
    api_resp,
    _inserted_timestamp
FROM
    batched
