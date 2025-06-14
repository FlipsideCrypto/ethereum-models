{{ config(
    materialized = 'table',
    unique_key = 'nft_address',
    tags = ['bronze','nft','nft_list']
) }}

WITH daily_trending_list AS (

    SELECT
        contract_address AS nft_address,
        SUM(price_usd) AS sale_usd
    FROM
        {{ ref('nft__ez_nft_sales') }}
    WHERE
        block_timestamp :: DATE >= CURRENT_DATE - 3
        AND nft_address NOT IN (
            '0x0e3a2a1f2146d86a604adc220b4967a898d7fe07',
            -- gods unchained
            '0x564cb55c655f727b61d9baf258b547ca04e9e548',
            -- gods unchained
            '0x6ebeaf8e8e946f0716e6533a6f2cefc83f60e8ab',
            --gods unchained
            '0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85' -- ENS
        )
        AND nft_address NOT IN (
            SELECT
                nft_address
            FROM
                {{ source(
                    'ethereum_silver',
                    'nft_collection_metadata'
                ) }}
        )
    GROUP BY
        ALL qualify ROW_NUMBER() over (
            ORDER BY
                sale_usd DESC
        ) <= 10
),
mints AS (
    SELECT
        contract_address AS nft_address,
        COUNT(1) AS mint_count
    FROM
        {{ ref('nft__ez_nft_transfers') }}
    WHERE
        is_mint
        AND nft_address IN (
            SELECT
                nft_address
            FROM
                daily_trending_list
        )
    GROUP BY
        ALL
    HAVING
        mint_count <= 50000
),
nft_list_from_mints AS (
    SELECT
        nft_address,
        ROW_NUMBER() over (
            ORDER BY
                mint_count DESC
        ) AS item_row_number
    FROM
        daily_trending_list
        INNER JOIN mints USING (nft_address) qualify ROW_NUMBER() over (
            ORDER BY
                mint_count DESC
        ) <= 20
),
build_req AS (
    SELECT
        nft_address,
        item_row_number,
        1 AS current_page,
        'qn_fetchNFTsByCollection' AS method,
        utils.udf_json_rpc_call(
            'qn_fetchNFTsByCollection',
            [{'collection': nft_address, 'page': 1, 'perPage': 100}]
        ) AS rpc_request
    FROM
        nft_list_from_mints
),
requests AS ({% for item in range(10) %}
    (
SELECT
    nft_address, item_row_number, live.udf_api('POST', CONCAT('{service}', '/', '{Authentication}'),{}, rpc_request, 'Vault/prod/ethereum/quicknode/mainnet') AS resp, SYSDATE() AS _inserted_timestamp
FROM
    build_req
WHERE
    item_row_number = {{ item }} + 1) {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}),
flattened AS (
    SELECT
        nft_address,
        INDEX,
        VALUE,
        VALUE :description :: STRING AS description,
        VALUE :traits AS traits
    FROM
        requests,
        LATERAL FLATTEN (
            input => resp :data :result :tokens
        )
),
traits_filter AS (
    SELECT
        nft_address,
        SUM(
            IFF(
                traits = '[]'
                OR traits IS NULL,
                0,
                1
            )
        ) AS traits_count,
        SUM(
            IFF(
                description = ''
                OR description IS NULL,
                0,
                1
            )
        ) AS description_count,
        IFF(
            description_count = 0
            OR traits_count IS NULL,
            'no',
            'yes'
        ) AS is_description_available,
        IFF(
            traits_count = 0
            OR description_count IS NULL,
            'no',
            'yes'
        ) AS are_traits_available
    FROM
        flattened
    GROUP BY
        nft_address
)
SELECT
    nft_address
FROM
    traits_filter
WHERE
    is_description_available = 'yes'
    OR are_traits_available = 'yes'
