{{ config(
    materialized = 'incremental',
    unique_key = 'aggregator_address'
) }}

WITH traces_base AS (

    SELECT
        from_address AS feed_address,
        to_address AS aggregator_address,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM
        {{ ref('silver__traces') }}
        t
        JOIN (
            SELECT
                feed_address
            FROM
                {{ ref('silver__chainlink_feeds_seed') }}
        ) b
        ON t.from_address = b.feed_address
    WHERE
        TYPE = 'STATICCALL'
        AND tx_status = 'SUCCESS'
        AND LEFT(
            input,
            10
        ) = '0x50d25bcd' -- latestAnswer

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    1,
    2
)
SELECT
    feed_address,
    aggregator_address,
    _inserted_timestamp
FROM
    traces_base
