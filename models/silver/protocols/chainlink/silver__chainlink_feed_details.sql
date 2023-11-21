{{ config(
    materialized = 'table'
) }}

SELECT
    feed_address,
    feed_name,
    feed_category,
    feed_added AS feed_added_date,
    created_block_number,    
    {{ dbt_utils.generate_surrogate_key(
        ['feed_address']
    ) }} AS chainlink_feed_details_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__chainlink_feeds_seed') }}
