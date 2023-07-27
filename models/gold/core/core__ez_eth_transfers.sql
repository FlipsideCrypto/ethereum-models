{{ config(
    materialized = 'incremental',
    persist_docs ={ "relation": true,
    "columns": true },
    unique_key = '_call_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['core','real_time'],
    post_hook = "{{ grant_data_share_statement('EZ_ETH_TRANSFERS', 'TABLE') }}"
) }}

WITH eth_base AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        from_address,
        to_address,
        eth_value,
        identifier,
        _call_id,
        _inserted_timestamp,
        input
    FROM
        {{ ref('silver__traces') }}
    WHERE
        TYPE = 'CALL'
        AND eth_value > 0
        AND tx_status = 'SUCCESS'
        AND trace_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
eth_price AS (
    SELECT
        HOUR,
        price AS eth_price
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        AND HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                eth_base
        )
),
tx_table AS (
    SELECT
        tx_hash,
        from_address AS origin_from_address,
        to_address AS origin_to_address,
        origin_function_signature
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                eth_base
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    A.tx_hash AS tx_hash,
    A.block_number AS block_number,
    A.block_timestamp AS block_timestamp,
    A.identifier AS identifier,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    A.from_address AS eth_from_address,
    A.to_address AS eth_to_address,
    A.eth_value AS amount,
    ROUND(
        A.eth_value * eth_price,
        2
    ) AS amount_usd,
    _call_id,
    _inserted_timestamp
FROM
    eth_base A
    LEFT JOIN eth_price
    ON DATE_TRUNC(
        'hour',
        block_timestamp
    ) = HOUR
    LEFT JOIN tx_table
    ON A.tx_hash = tx_table.tx_hash qualify(ROW_NUMBER() over (PARTITION BY _call_id
ORDER BY
    _inserted_timestamp DESC)) = 1
