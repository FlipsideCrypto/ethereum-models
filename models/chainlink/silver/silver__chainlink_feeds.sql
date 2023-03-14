{{ config(
    materialized = 'incremental',
    unique_key = '_log_id'
) }}

WITH contracts_base AS (

    SELECT
        feed_address,
        aggregator_address
    FROM
        {{ ref('silver__chainlink_aggregators') }}
),
logs_base AS (
    SELECT
        block_number,
        block_timestamp,
        PUBLIC.udf_hex_to_int(
            topics [1] :: STRING
        ) :: INT AS answer,
        PUBLIC.udf_hex_to_int(
            topics [2] :: STRING
        ) :: INT AS round_id,
        TO_TIMESTAMP_NTZ(PUBLIC.udf_hex_to_int(DATA :: STRING) :: INT) AS updated_at,
        feed_address,
        _inserted_timestamp,
        _log_id
    FROM
        {{ ref('silver__logs') }} A
        JOIN contracts_base b
        ON A.contract_address = b.aggregator_address
    WHERE
        tx_status = 'SUCCESS'
        AND topics [0] :: STRING = '0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f'

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
    feed_address,
    round_id,
    answer,
    updated_at,
    _inserted_timestamp,
    _log_id
FROM
    logs_base
WHERE
    answer IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
