{{ config(
    materialized = 'incremental',
    unique_key = 'collection_page',
    tags = ['nft_metadata']
) }}
-- full_refresh = false
WITH raw AS (

    SELECT
        *
    FROM
        {{ ref('bronze_api__nft_metadata_list_request') }}

{% if is_incremental() %}
WHERE
    collection_page NOT IN (
        SELECT
            collection_page
        FROM
            {{ this }}
    ) --     AND nft_address != '0x60f80121c31a0d46b5279700f9df786054aa5ee5'
    --     AND collection_page NOT IN ('0xd07dc4262bcdbf85190c01c996b4c06a461d2430-1740') -- rarible 3 sunflowers
    -- {% endif %}
),
numbered AS (
    SELECT
        *,
        (
            SELECT
                TRY(
                    ethereum.streamline.udf_api('POST', node_url, {}, PARSE_JSON(json_request))
                )
                ON ERROR 'FAILED'
        ) AS api_resp
    FROM
        raw
    qualify ROW_NUMBER() over (ORDER BY collection_page ASC) <= 50
),
requests AS (
    -- Use a loop to iterate over the range
    {% for item in range(10) %}
    (
        SELECT
            nft_address,
            current_page,
            end_page,
            collection_page,
            row_num,
            (
                CASE
                    WHEN {{ item }} = 0 THEN NULL -- First run, no need to skip
                    WHEN prev_api_resp IS NOT NULL AND prev_api_resp = 'FAILED' THEN NULL -- Skip if the previous run failed
                    ELSE
                        -- Try the API call
                        CASE
                            WHEN {{ item }} = 0 OR (prev_api_resp IS NOT NULL AND prev_api_resp = 'SUCCESS') THEN
                                (
                                    {% if item > 0 %}
                                    {% if prev_api_resp == 'FAILED' %}
                                    NULL
                                    {% else %}
                                    TRY(
                                        ethereum.streamline.udf_api('POST', node_url, {}, PARSE_JSON(json_request))
                                    )
                                    {% endif %}
                                    {% else %}
                                    TRY(
                                        ethereum.streamline.udf_api('POST', node_url, {}, PARSE_JSON(json_request))
                                    )
                                    {% endif %}
                                )
                            ELSE NULL
                        END
                END
            ) AS api_resp,
            SYSDATE() AS _inserted_timestamp
        FROM (
            SELECT
                nft_address,
                current_page,
                end_page,
                collection_page,
                row_num,
                LAG(api_resp) OVER (ORDER BY row_num) AS prev_api_resp
            FROM numbered
        ) subquery
        WHERE
            row_num BETWEEN ({{ item }} * 5 + 1) AND ((({{ item }} + 1) * 5))
    ) 
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
SELECT
    *
FROM
    requests