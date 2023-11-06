{{ config(
    materialized = 'incremental',
    unique_key = 'collection_page',
    tags = ['nft_metadata'],
    full_refresh = false
) }}
{# WITH raw AS (

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
#}
WITH nft_mints AS (
    -- temporary change to manually add cryptopunks
    SELECT
        '0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb' AS nft_address,
        10000 AS mint_count,
        1 AS start_page,
        CEIL(
            mint_count / 100
        ) AS end_page
    LIMIT
        1
), generator_table AS (
    SELECT
        ROW_NUMBER() over (
            ORDER BY
                SEQ4()
        ) AS full_rows
    FROM
        TABLE(GENERATOR(rowcount => 10000))
),
nft_address_x_list_of_pages AS (
    SELECT
        nft_address,
        mint_count,
        start_page,
        end_page,
        full_rows AS current_page,
        'qn_fetchNFTsByCollection' AS method,
        CONCAT(
            nft_address,
            '-',
            current_page
        ) AS collection_page
    FROM
        nft_mints
        CROSS JOIN generator_table
    WHERE
        full_rows BETWEEN start_page
        AND end_page
),
raw AS (
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
        node_url
    FROM
        nft_address_x_list_of_pages
        JOIN {{ source(
            'streamline_crosschain',
            'node_mapping'
        ) }}
        ON 1 = 1
    WHERE
        chain = 'ethereum'
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
    nft_address, current_page, end_page, collection_page, row_num, ethereum.streamline.udf_api('POST', node_url,{}, PARSE_JSON(json_request)) AS api_resp, SYSDATE() AS _inserted_timestamp
FROM
    numbered

{% if is_incremental() %}
WHERE
    row_num BETWEEN ({{ item }} * 10 + 1)
    AND ((({{ item }} + 1) * 10))
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
