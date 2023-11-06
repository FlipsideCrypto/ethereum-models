{{ config(
    materialized = 'incremental',
    unique_key = 'collection_page',
    tags = ['nft_list']
) }}

WITH nft_list AS (

    SELECT
        nft_address
    FROM
        {{ ref('bronze_api__nft_metadata_list_filter') }}

{% if is_incremental() %}
WHERE
    nft_address NOT IN (
        SELECT
            nft_address
        FROM
            {{ this }}
    )
{% endif %}
UNION
SELECT
    nft_address
FROM
    {{ source(
        'crosschain_public',
        'user_metadata'
    ) }}

{% if is_incremental() %}
WHERE
    nft_address NOT IN (
        SELECT
            nft_address
        FROM
            {{ this }}
    )
{% endif %}
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
        INNER JOIN nft_list USING (nft_address)
    GROUP BY
        nft_address
    HAVING
        mint_count <= 50000
),
nft_list_backdoor AS (
    SELECT
        nft_address,
        mint_count,
        start_page,
        end_page
    FROM
        {{ ref('bronze_api__nft_metadata_list_backdoor') }}

{% if is_incremental() %}
WHERE
    nft_address NOT IN (
        SELECT
            nft_address
        FROM
            {{ this }}
    )
{% endif %}
),
nft_mints_all AS (
    SELECT
        *
    FROM
        nft_mints
    UNION ALL
    SELECT
        *
    FROM
        nft_list_backdoor
),
generator_table AS (
    SELECT
        ROW_NUMBER() over (
            ORDER BY
                SEQ4()
        ) AS full_rows
    FROM
        TABLE(GENERATOR(rowcount => 50000))
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
        nft_mints_all
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
    SYSDATE() AS request_inserted_timestamp
FROM
    nft_address_x_list_of_pages
    JOIN {{ source(
        'streamline_crosschain',
        'node_mapping'
    ) }}
    ON 1 = 1
WHERE
    chain = 'ethereum'
