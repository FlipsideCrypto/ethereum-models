{{ config(
    materialized = 'incremental',
    unique_key = "price_id",
    cluster_by = ['prices_hour::DATE'],
    tags = ['non_realtime']
) }}

WITH base AS (

    SELECT
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS asset,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS withdraw_amount,*
    FROM
        {{ ref('silver__logs') }}
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON asset = C.address
    WHERE
        contract_address IN (
            '0xa17581a9e3356d9a858b789d68b4d866e593ae94',
            '0xc3d688b66703497daa19211eedff47f25384cdc3'
        )
),
prices AS (
    SELECT
        HOUR,
        token_address,
        AVG(price) AS price
    FROM
        {{ ref('silver__hourly_prices_all_providers') }}
    WHERE
        (token_address IN (
            SELECT
                DISTINCT(asset)
            FROM
                base
        )
        OR token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')

{% if is_incremental() %}
AND HOUR :: DATE >= (
    SELECT
        MAX(
            prices_hour
        ) - INTERVAL '36 hours'
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    1,
    2
),
date_expand AS (
    SELECT
        DISTINCT HOUR
    FROM
        prices
),
addresses AS (
    SELECT
        DISTINCT asset
    FROM
        base
),
address_list AS (
    SELECT
        *
    FROM
        date_expand
        JOIN addresses
),
eth_prices AS (
    SELECT
        HOUR AS eth_price_hour,
        price AS eth_price
    FROM
        prices
    WHERE
        token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
),
prices_join AS (
    SELECT
        al.hour AS prices_hour,
        al.asset,
        NAME,
        symbol,
        decimals,
        eth_price,
        price AS hourly_price,
        concat_ws(
            '-',
            al.hour,
            al.asset
        ) AS price_id
    FROM
        address_list al
        LEFT JOIN base am
        ON al.asset = am.asset
        LEFT JOIN prices p
        ON al.asset = p.token_address
        AND al.hour = p.hour
        LEFT JOIN eth_prices
        ON eth_price_hour = al.hour
)
SELECT
    *
FROM
    prices_join
WHERE
    hourly_price IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY price_id
ORDER BY
    hourly_price DESC)) = 1
