{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    tags = ['non_realtime'],
) }}

WITH logs AS (

    SELECT
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CASE
            WHEN topics [0] = '0xb7f7e57b7bb3a5186ad1bd43405339ba361555344aec7a4be01968e88ee3883e' THEN CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40))
            WHEN topics [0] = '0x9303649990c462969a3c46d4e2c758166e92f5a4b18c67f26d3e58d2b0660e67' THEN CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 42))
        END AS pool_address,*
    FROM
        {{ ref('silver__logs') }}
    WHERE
        origin_from_address IN (
            '0xdb3388e770f49a604e11f1a2084b39279492a61f',
            '0xf4e1d185666a624099298fcc42c50ba662dc7e52',
            '0xaa913c26dd7723fcae9dbd2036d28171a56c6251'
        )
        AND topics [0] IN (
            '0xb7f7e57b7bb3a5186ad1bd43405339ba361555344aec7a4be01968e88ee3883e',
            '0x9303649990c462969a3c46d4e2c758166e92f5a4b18c67f26d3e58d2b0660e67',
            '0xc6fa598658c9cdf9eaa5f76414ef17a38a7f74c0e719a0571a3f73d9ecd755b7'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '36 hours'
    FROM
        {{ this }}
)
{% endif %}
),
logs_transform AS (
    SELECT
        pool_address AS frax_market_address,
        NAME AS frax_market_name,
        symbol AS frax_market_symbol,
        decimals,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 42)) AS underlying_asset,
        l._log_id,
        l._inserted_timestamp
    FROM
        logs l
        LEFT JOIN ethereum_dev.silver.contracts
        ON address = pool_address
)
SELECT
    frax_market_address,
    frax_market_name,
    CASE
        WHEN frax_market_address = '0x1fff4a418471a7b44efa023320e02dcdb486ed77' THEN 'crvFRAX'
        WHEN frax_market_address = '0x21e354da5d929a4da55428f6bfd71ed9ffd5001f' THEN 'sfrxETH'
        WHEN frax_market_address = '0x281e6cb341a552e4faccc6b4eef1a6fcc523682d' THEN 'FRAX-frxETH/ETH Curve LP'
        WHEN frax_market_address = '0x32467a5fc2d72d21e8dce990906547a2b012f382' THEN 'WBTC/FRAX'
        WHEN frax_market_address = '0x3835a58ca93cdb5f912519ad366826ac9a752510' THEN 'CRV/FRAX'
        WHEN frax_market_address = '0x3a25b9ab8c07ffefee614531c75905e810d8a239' THEN 'APE'
        WHEN frax_market_address = '0x50e627a1df8d665524942ad7ec6392b6ba60293a' THEN 'FPI'
        WHEN frax_market_address = '0x66bf36dba79d4606039f04b32946a260bcd3ff52' THEN 'gOHM'
        WHEN frax_market_address = '0x6cd3600998b094d4d5cb7b1d3cb6d11b8e842213' THEN 'UNI/FRAX'
        WHEN frax_market_address = '0x74f82bd9d0390a4180daaec92d64cf0708751759' THEN 'FPI'
        WHEN frax_market_address = '0x78bb3aec3d855431bd9289fd98da13f9ebb7ef15' THEN 'sfrxETH'
        WHEN frax_market_address = '0x794f6b13fbd7eb7ef10d1ed205c9a416910207ff' THEN 'WETH/FRAX'
        WHEN frax_market_address = '0x82ec28636b77661a95f021090f6be0c8d379dd5d' THEN 'MKR'
        WHEN frax_market_address = '0x918ac498470d073c6518b55f3f74c203d6df38fd' THEN 'FPI'
        WHEN frax_market_address = '0xa1d100a5bf6bfd2736837c97248853d989a9ed84' THEN 'CVX/FRAX'
        WHEN frax_market_address = '0xa96eb85d1afb297bb67eb4856ad5a469ec921d00' THEN 'sfrxETH'
        WHEN frax_market_address = '0xc4d57603d47fb842ec11a5332748f9f96d44cbeb' THEN 'CVX/FRAX'
        WHEN frax_market_address = '0xc6cada314389430d396c7b0c70c6281e99ca7fe8' THEN 'UNI'
        WHEN frax_market_address = '0xc779fee076eb04b9f8ea424ec19de27efd17a68d' THEN 'AAVE'
        WHEN frax_market_address = '0xdbe88dbac39263c47629ebba02b3ef4cf0752a72' THEN 'FXS/FRAX'
        WHEN frax_market_address = '0xe1b6a8c4a044b38fcd862ba509844c54393cc737' THEN 'sfrxETH'
        ELSE frax_market_symbol
    END AS frax_market_symbol,
    decimals,
    underlying_asset,
    _log_id,
    _inserted_timestamp
FROM
    logs_transform
