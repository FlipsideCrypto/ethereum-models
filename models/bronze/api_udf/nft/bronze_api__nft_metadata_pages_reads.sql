{{ config(
    materialized = 'incremental',
    unique_key = 'nft_address'
) }}

WITH nft_collection AS (

    SELECT
        nft_address
    FROM
        {{ ref('bronze_api__top_nft_collection') }}

{% if is_incremental() %}
WHERE
    nft_address NOT IN (
        SELECT
            nft_address
        FROM
            {{ this }}
    )
{% endif %}
LIMIT
    50
), input_data AS (
    SELECT
        nft_address AS contract_address,
        'qn_fetchNFTsByCollection' AS method,
        ROW_NUMBER() over (
            ORDER BY
                contract_address
        ) AS row_no,
        FLOOR(
            row_no / 5
        ) + 1 AS batch_no,
        node_url
    FROM
        nft_collection
        JOIN {{ source(
            'streamline_crosschain',
            'node_mapping'
        ) }}
        ON 1 = 1
    WHERE
        chain = 'ethereum'
),
ready_requests_raw AS (
    SELECT
        CONCAT(
            '{\'id\': 67, \'jsonrpc\': \'2.0\', \'method\': \'',
            method,
            '\',\'params\': [{ \'collection\': \'',
            contract_address,
            '\', \'omitFields\': [ \'imageUrl\' , \'name\', \'collectionAddress\'], \'page\': 1 ,\'perPage\': 100 }]}'
        ) AS json_request,
        node_url,
        batch_no
    FROM
        input_data
),
batched AS ({% for item in range(10) %}
SELECT
    ethereum.streamline.udf_api('POST', node_url,{}, PARSE_JSON(json_request)) AS api_resp
FROM
    ready_requests_raw
WHERE
    batch_no = {{ item }}
    AND EXISTS (
SELECT
    1
FROM
    input_data
LIMIT
    1) {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}),
node_results_overview AS (
    SELECT
        api_resp :data :result :collection :: STRING AS nft_address,
        api_resp :data :result :pageNumber :: INTEGER AS pageNumber,
        api_resp :data :result :totalItems :: INTEGER AS totalItems,
        api_resp :data :result :totalPages :: INTEGER AS totalPages,
        api_resp :data :result :tokens [0] :collectionName :: STRING AS collection_name,
        api_resp :data :result :tokens [0] :chain :: STRING AS chain,
        api_resp :data :result :tokens [0] :network :: STRING AS network,
        api_resp AS full_data
    FROM
        batched
),
node_results_flatten AS (
    SELECT
        nft_address,
        pageNumber,
        totalItems,
        totalPages,
        collection_name,
        chain,
        network,
        LISTAGG(
            VALUE :traits
        ) AS traits_value
    FROM
        node_results_overview,
        LATERAL FLATTEN(
            input => full_data :data :result :tokens
        )
    GROUP BY
        nft_address,
        pageNumber,
        totalItems,
        totalPages,
        collection_name,
        chain,
        network
)
SELECT
    nft_address,
    pageNumber,
    totalItems,
    totalPages,
    collection_name,
    chain,
    network,
    traits_value,
    SYSDATE() AS _inserted_timestamp
FROM
    node_results_flatten
