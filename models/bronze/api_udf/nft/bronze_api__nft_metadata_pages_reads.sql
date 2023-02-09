{{ config(
    materialized = 'incremental',
    unique_key = 'nft_address',
    full_refresh = false
) }}

WITH nft_collection as (
    SELECT
        nft_address
    FROM
        {{ ref('bronze_api__top_nft_collection')}}

{% if is_incremental() %}
WHERE nft_address NOT IN (
    SELECT
        nft_address
    FROM
        {{ this }}
)
{% endif %}   

LIMIT 150
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
    api_resp :data :result :collection :: STRING AS nft_address,
    api_resp :data :result :pageNumber :: INTEGER AS pageNumber,
    api_resp :data :result :totalItems :: INTEGER AS totalItems,
    api_resp :data :result :totalPages :: INTEGER AS totalPages,
    api_resp AS full_data,
    SYSDATE() as _inserted_timestamp
FROM
    node_results
)

SELECT 
    * 
FROM 
    node_results_overview
WHERE 
    totalItems > 0  
