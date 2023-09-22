{{ config(
    materialized = 'incremental',
    unique_key = 'collection_page'
) }}


WITH raw AS (
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
{% endif %}

limit 200
),

numbered as (
    select
        *, 
        ROW_NUMBER() over (order by nft_address, current_page ASC) as row_number_new
    from raw 
),

requests AS ({% for item in range(5) %}
    (
SELECT
    nft_address, 
    current_page, 
    end_page, 
    collection_page, 
    row_num,
    row_number_new,
    ethereum.streamline.udf_api('POST', node_url,{}, PARSE_JSON(json_request)) AS api_resp, 
    SYSDATE() AS _inserted_timestamp
FROM
    numbered

{% if is_incremental() %}
WHERE
    row_number_new BETWEEN ({{ item }} * 40 + 1 )
    AND ((({{ item }} + 1) * 40 )) 
{% else %}
WHERE
    row_number_new BETWEEN ({{ item }} * 50 + 1)
    AND ((({{ item }} + 1) * 50 )) 
{% endif %}) {% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %})
SELECT
    *
FROM
    requests
