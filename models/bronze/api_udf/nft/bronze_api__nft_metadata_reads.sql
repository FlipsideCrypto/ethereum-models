{{ config(
    materialized = 'incremental',
    unique_key = 'collection_page'
) }}

WITH max_row_number as (

{% if is_incremental() %}

SELECT
    MAX(row_num)::int AS max_row_num
FROM
    {{ this }}
{% else %}
SELECT
    0 AS max_row_num
{% endif %}), 


 requests AS (
    {% for item in range(5) %}
    (

    SELECT
        nft_address, 
        current_page, 
        end_page, 
        collection_page, 
        row_num, 
        max_row_num,
        --ethereum.streamline.udf_api('POST', node_url,{}, PARSE_JSON(json_request)) AS api_resp, 
        SYSDATE() AS _inserted_timestamp
    FROM
        {{ ref('bronze_api__nft_metadata_list') }}
        JOIN max_row_number
        on 1 = 1 

    
    {% if is_incremental() %}
    where row_num BETWEEN ({{ item }} * 10 + 1 + max_row_num)  -- + max_row_num
    AND ((({{ item }} + 1) * 10 + max_row_num)   ) -- + max_row_num
    
    {% else %}
    where row_num BETWEEN ({{ item }} * 30 + 1 )  --+ max_row_num
            AND ((({{ item }} + 1) * 30)   ) --+ max_row_num

    {% endif %}
) 

{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
)
 

SELECT * from requests
    
   