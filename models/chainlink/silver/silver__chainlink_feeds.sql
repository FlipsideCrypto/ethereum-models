{{ config(
    materialized = 'incremental',
    unique_key = 'id'
) }}

WITH contracts_base AS (

    SELECT
        feed_address,
        feed_name
    FROM
        {{ ref('silver__chainlink_feeds_seed') }}
),
traces_base AS (
    SELECT
        block_number,
        block_timestamp,
        feed_name,
        feed_address,
        _inserted_timestamp,
        regexp_substr_all(SUBSTR(output, 3, len(output)), '.{64}') AS segmented_output
    FROM
        {{ ref('silver__traces') }} A
        JOIN contracts_base b
        ON A.from_address = b.feed_address
    WHERE
        LEFT(
            input,
            10
        ) = '0xfeaf968c' -- latestRoundData
        AND tx_status = 'SUCCESS'

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
)
SELECT
    block_number,
    block_timestamp,
    feed_name,
    feed_address,
    PUBLIC.udf_hex_to_int(
        segmented_output [0] :: STRING
    ) :: INTEGER AS round_id,
    PUBLIC.udf_hex_to_int(
        segmented_output [1] :: STRING
    ) :: INTEGER AS answer,
    TO_TIMESTAMP_NTZ(
        PUBLIC.udf_hex_to_int(
            segmented_output [2] :: STRING
        ) :: INTEGER
    ) AS started_at,
    TO_TIMESTAMP_NTZ(
        PUBLIC.udf_hex_to_int(
            segmented_output [3] :: STRING
        ) :: INTEGER
    ) AS updated_at,
    PUBLIC.udf_hex_to_int(
        segmented_output [4] :: STRING
    ) :: INTEGER AS answered_in_round,
    _inserted_timestamp,
    CONCAT(
        block_number :: STRING,
        '-',
        feed_address
    ) AS id
FROM
    traces_base
WHERE
    answer IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
