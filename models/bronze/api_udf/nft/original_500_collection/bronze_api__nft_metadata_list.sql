{{ config(
    materialized = 'table',
    unique_key = 'collection_page'
) }}

WITH top_nft_collection AS (

    SELECT
        nft_address,
        SUM(price_usd) AS usd_sales
    FROM
        {{ ref('nft__ez_nft_sales') }}
    WHERE
        nft_address NOT IN (
            '0x0e3a2a1f2146d86a604adc220b4967a898d7fe07',
            -- gods unchained
            '0x564cb55c655f727b61d9baf258b547ca04e9e548',
            -- gods unchained
            '0x6ebeaf8e8e946f0716e6533a6f2cefc83f60e8ab',
            --gods unchained
            '0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85' -- ENS
        )
    GROUP BY
        nft_address qualify ROW_NUMBER() over (
            ORDER BY
                SUM(price_usd) DESC
        ) <= 500
),
nft_mints AS (
    SELECT
        nft_address,
        COUNT(
            DISTINCT tokenid
        ) AS mint_count,
        1 AS start_page,
        CEIL(
            mint_count / 100
        ) AS end_page
    FROM
        {{ ref('nft__ez_nft_mints') }}
        INNER JOIN top_nft_collection USING (nft_address)
    GROUP BY
        nft_address
),
generator_table AS (
    SELECT
        ROW_NUMBER() over (
            ORDER BY
                SEQ4()
        ) AS full_rows
    FROM
        TABLE(GENERATOR(rowcount => 100000))
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
)
SELECT
    nft_address,
    current_page,
    end_page,
    collection_page,
    utils.udf_json_rpc_call(
        'qn_fetchNFTsByCollection',
        [{'collection': nft_address, 'page': current_page, 'perPage': 100}]
    ) AS json_request
FROM
    nft_address_x_list_of_pages
