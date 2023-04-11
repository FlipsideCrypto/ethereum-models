{{ config(
    materialized = 'incremental',
    unique_key = 'collection_page',
    full_refresh = false
) }}

WITH input_data_detailed AS (

    SELECT
        totalPages AS total_pages,
        nft_address,
        'qn_fetchNFTsByCollection' AS method
    FROM
        {{ ref('bronze_api__nft_metadata_pages_reads') }}
    WHERE
        traits_value LIKE '%trait_type%'
),
generate_series AS (
    SELECT
        SEQ4() + 1 AS page_plug
    FROM
        TABLE(GENERATOR(rowcount => 5000))
),
limit_series AS (
    SELECT
        page_plug,
        nft_address,
        CONCAT(
            nft_address,
            '-',
            page_plug
        ) AS collection_page,
        method
    FROM
        generate_series
        JOIN input_data_detailed
        ON total_pages >= page_plug

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
    150
), ready_requests AS (
    SELECT
        page_plug,
        nft_address,
        collection_page,
        CONCAT(
            '{\'id\': 67, \'jsonrpc\': \'2.0\', \'method\': \'',
            method,
            '\',\'params\': { \'collection\': \'',
            nft_address,
            '\', \'page\': ',
            page_plug,
            ',\'perPage\': 100 } }'
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
        limit_series
        JOIN {{ source(
            'streamline_crosschain',
            'node_mapping'
        ) }}
        ON 1 = 1
    WHERE
        chain = 'ethereum'
),
batched AS ({% for item in range(50) %}
SELECT
    nft_address, collection_page, ethereum.streamline.udf_api('POST', node_url,{}, PARSE_JSON(json_request)) AS api_resp, SYSDATE() AS _inserted_timestamp
FROM
    ready_requests
WHERE
    batch_no = {{ item }}
    AND EXISTS (
SELECT
    1
FROM
    ready_requests
LIMIT
    1) {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %})
SELECT
    nft_address,
    collection_page,
    api_resp,
    _inserted_timestamp
FROM
    batched
