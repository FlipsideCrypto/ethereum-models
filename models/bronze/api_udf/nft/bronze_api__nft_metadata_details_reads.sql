{{ config(
    materialized = 'incremental',
    unique_key = 'collection_page'
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
        TABLE(GENERATOR(rowcount => 10000))
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
    100
), ready_requests AS (
    SELECT
        page_plug,
        nft_address,
        collection_page,
        CONCAT(
            '{\'id\': 1, \'jsonrpc\': \'2.0\', \'method\': \'',
            method,
            '\',\'params\': { \'collection\': \'',
            nft_address,
            '\', \'page\': ',
            page_plug,
            '}}'
        ) AS json_request
    FROM
        limit_series
),
node_details AS (
    SELECT
        *
    FROM
        {{ source(
            'streamline_crosschain',
            'node_mapping'
        ) }}
    WHERE
        chain = 'ethereum'
),
node_output AS (
    SELECT
        nft_address,
        collection_page,
        ethereum.streamline.udf_api(
            'POST',
            node_url,{},
            PARSE_JSON(json_request)
        ) AS api_resp,
        SYSDATE() AS _inserted_timestamp
    FROM
        ready_requests
        JOIN node_details
        ON 1 = 1
)
SELECT
    nft_address,
    collection_page,
    api_resp,
    _inserted_timestamp
FROM
    node_output
