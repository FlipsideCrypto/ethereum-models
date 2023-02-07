{{ config(
    materialized = 'incremental',
    unique_key = 'collection_page',
    full_refresh = false
) }}

WITH nft_collection as (
    SELECT
        LOWER(nft_address) ::STRING as nft_address,
        SUM(price) as total_price
    FROM
        {{ ref('core__ez_nft_sales')}}
    WHERE 
        currency_symbol in ('ETH', 'WETH')
    AND 
        platform_name in ('opensea', 'blur')

    GROUP BY
        nft_address
    QUALIFY ROW_NUMBER() OVER (ORDER BY total_price DESC) <= 2
),

input_data AS (
    SELECT
        nft_address AS contract_address,
        'qn_fetchNFTsByCollection' AS method
    FROM
        nft_collection
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

ready_requests_raw AS (
    SELECT
        CONCAT(
            '{\'id\': 1, \'jsonrpc\': \'2.0\', \'method\': \'',
            method,
            '\',\'params\': { \'collection\': \'',
            contract_address,
            '\', \'omitFields\': [ \'imageUrl\' , \'traits\', \'description\', \'name\', \'collectionName\', \'collectionAddress\'], \'page\': 1}}'
        ) AS json_request
    FROM
        input_data
),

node_results as (
SELECT
    ethereum.streamline.udf_api(
        'POST',
        node_url,{},
        PARSE_JSON(json_request)
    ) AS api_resp
FROM
    ready_requests_raw
    JOIN node_details
    ON 1 = 1
),

node_results_overview as (

SELECT
    api_resp :data :result :collection :: STRING AS collection,
    api_resp :data :result :pageNumber :: INTEGER AS pageNumber,
    api_resp :data :result :totalItems :: INTEGER AS totalItems,
    api_resp :data :result :totalPages :: INTEGER AS totalPages,
    totalItems / totalPages,
    api_resp AS full_data
FROM
    node_results
),

input_data_detailed as (
SELECT
    totalPages as total_pages,
    collection,
    'qn_fetchNFTsByCollection' AS method
FROM
        node_results_overview
),

generate_series AS (
    SELECT
        SEQ4() + 1 AS page_plug
    FROM
        TABLE(GENERATOR(rowcount => 20000))
),

limit_series AS (
    SELECT
        page_plug,
        collection,
        method
    FROM
        generate_series
        JOIN input_data_detailed
        ON total_pages >= page_plug
) ,

ready_requests AS (
        SELECT
            CONCAT(collection, '-', page_plug) AS collection_page,
            CONCAT(
                '{\'id\': 1, \'jsonrpc\': \'2.0\', \'method\': \'',
                method,
                '\',\'params\': { \'collection\': \'',
                collection,
                '\', \'page\': ',
                page_plug,
                '}}'
            ) AS json_request
        FROM
            limit_series
),

 FINAL AS (
        SELECT
            collection_page,
            ethereum.streamline.udf_api(
                'POST',
                node_url,{},
                PARSE_JSON(json_request)
            ) AS api_resp
        FROM
            ready_requests
            JOIN node_details
            ON 1 = 1

{% if is_incremental() %}
WHERE collection_page NOT IN (
    SELECT
        collection_page
    FROM
        {{ this }}
)
{% endif %}

limit 50

)
    -- page plug 99 -> 1min 42s, 3960 rows

SELECT 
    * 
FROM 
    FINAL