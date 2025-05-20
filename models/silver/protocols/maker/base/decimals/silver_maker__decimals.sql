{{ config(
    materialized = 'view',
    tags = ['silver','curated','maker']
) }}

WITH base AS (

    SELECT
        'AAVE' AS symbol,
        LOWER('0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9') AS token_address,
        18 AS decimals
    UNION ALL
    SELECT
        'BAL' AS symbol,
        LOWER('0xba100000625a3754423978a60c9317c58a424e3d') AS token_address,
        18 AS decimals
    UNION ALL
    SELECT
        'BAT' AS symbol,
        LOWER('0x0d8775f648430679a709e98d2b0cb6250d2887ef') AS token_address,
        18 AS decimals
    UNION ALL
    SELECT
        'COMP' AS symbol,
        LOWER('0xc00e94cb662c3520282e6f5717214004a7f26888') AS token_address,
        18 AS decimals
    UNION ALL
    SELECT
        'CRVV1ETHSTETH' AS symbol,
        LOWER('0x06325440D014e39736583c165C2963BA99fAf14E') AS token_address,
        18 AS decimals
    UNION ALL
    SELECT
        'ETH' AS symbol,
        LOWER('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2') AS token_address,
        18 AS decimals
    UNION ALL
    SELECT
        'GNO' AS symbol,
        LOWER('0x6810e776880c02933d47db1b9fc05908e5386b96') AS token_address,
        18 AS decimals
    UNION ALL
    SELECT
        'GUNIV3DAIUSDC1' AS symbol,
        LOWER('0xAbDDAfB225e10B90D798bB8A886238Fb835e2053') AS token_address,
        18 AS decimals
    UNION ALL
    SELECT
        'GUNIV3DAIUSDC2' AS symbol,
        LOWER('0x50379f632ca68D36E50cfBC8F78fe16bd1499d1e') AS token_address,
        18 AS decimals
    UNION ALL
    SELECT
        'GUSD' AS symbol,
        LOWER('0x056fd409e1d7a124bd7017459dfea2f387b6d5cd') AS token_address,
        2 AS decimals
    UNION ALL
    SELECT
        'KNC' AS symbol,
        LOWER('0xdd974d5c2e2928dea5f71b9825b8b646686bd200') AS token_address,
        18 AS decimals
    UNION ALL
    SELECT
        'LINK' AS symbol,
        LOWER('0x514910771af9ca656af840dff83e8264ecf986ca') AS token_address,
        18 AS decimals
    UNION ALL
    SELECT
        'LRC' AS symbol,
        LOWER('0xbbbbca6a901c926f240b89eacb641d8aec7aeafd') AS token_address,
        18 AS decimals
    UNION ALL
    SELECT
        'MANA' AS symbol,
        LOWER('0x0f5d2fb29fb7d3cfee444a200298f468908cc942') AS token_address,
        18 AS decimals
    UNION ALL
    SELECT
        'MATIC' AS symbol,
        LOWER('0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0') AS token_address,
        18 AS decimals
    UNION ALL
    SELECT
        'PAXUSD' AS symbol,
        LOWER('0x8e870d67f660d95d5be530380d0ec0bd388289e1') AS token_address,
        18 AS decimals
    UNION ALL
    SELECT
        'RENBTC' AS symbol,
        LOWER('0xeb4c2781e4eba804ce9a9803c67d0893436bb27d') AS token_address,
        8 AS decimals
    UNION ALL
    SELECT
        'RETH' AS symbol,
        LOWER('0x9559a440cde1c585c61872e0c5ed9f7dcd71840f') AS token_address,
        18 AS decimals
    UNION ALL
    SELECT
        'TUSD' AS symbol,
        LOWER('0x0000000000085d4780b73119b644ae5ecd22b376') AS token_address,
        18 AS decimals
    UNION ALL
    SELECT
        'UNI' AS symbol,
        LOWER('0x1f9840a85d5af5bf1d1762f925bdaddc4201f984') AS token_address,
        18 AS decimals
    UNION ALL
    SELECT
        'UNIV2AAVEETH' AS symbol,
        LOWER('0xDFC14d2Af169B0D36C4EFF567Ada9b2E0CAE044f') AS token_address,
        18 AS decimals
    UNION ALL
    SELECT
        'UNIV2DAIETH' AS symbol,
        LOWER('0xA478c2975Ab1Ea89e8196811F51A7B7Ade33eB11') AS token_address,
        18 AS decimals
    UNION ALL
    SELECT
        'UNIV2DAIUSDC' AS symbol,
        LOWER('0xAE461cA67B15dc8dc81CE7615e0320dA1A9aB8D5') AS token_address,
        18 AS decimals
    UNION ALL
    SELECT
        'UNIV2DAIUSDT' AS symbol,
        LOWER('0xB20bd5D04BE54f870D5C0d3cA85d82b34B836405') AS token_address,
        18 AS decimals
    UNION ALL
    SELECT
        'UNIV2ETHUSDT' AS symbol,
        LOWER('0x0d4a11d5EEaaC28EC3F61d100daF4d40471f1852') AS token_address,
        18 AS decimals
    UNION ALL
    SELECT
        'UNIV2LINKETH' AS symbol,
        LOWER('0xa2107FA5B38d9bbd2C461D6EDf11B11A50F6b974') AS token_address,
        18 AS decimals
    UNION ALL
    SELECT
        'UNIV2UNIETH' AS symbol,
        LOWER('0xd3d2E2692501A5c9Ca623199D38826e513033a17') AS token_address,
        18 AS decimals
    UNION ALL
    SELECT
        'UNIV2USDCETH' AS symbol,
        LOWER('0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc') AS token_address,
        18 AS decimals
    UNION ALL
    SELECT
        'UNIV2WBTCDAI' AS symbol,
        LOWER('0x231B7589426Ffe1b75405526fC32aC09D44364c4') AS token_address,
        18 AS decimals
    UNION ALL
    SELECT
        'UNIV2WBTCETH' AS symbol,
        LOWER('0xBb2b8038a1640196FbE3e38816F3e67Cba72D940') AS token_address,
        18 AS decimals
    UNION ALL
    SELECT
        'USDC' AS symbol,
        LOWER('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48') AS token_address,
        6 AS decimals
    UNION ALL
    SELECT
        'USDT' AS symbol,
        LOWER('0xdac17f958d2ee523a2206206994597c13d831ec7') AS token_address,
        6 AS decimals
    UNION ALL
    SELECT
        'WBTC' AS symbol,
        LOWER('0x2260fac5e5542a773aa44fbcfedf7c193bc2c599') AS token_address,
        8 AS decimals
    UNION ALL
    SELECT
        'WSTETH' AS symbol,
        LOWER('0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0') AS token_address,
        18 AS decimals
    UNION ALL
    SELECT
        'YFI' AS symbol,
        LOWER('0x0bc529c00c6401aef6d220be8c6ea1667f6ad93e') AS token_address,
        18 AS decimals
    UNION ALL
    SELECT
        'ZRX' AS symbol,
        LOWER('0xe41d2489571d322189246dafa5ebde1f4699f498') AS token_address,
        18 AS decimals
)
SELECT
    symbol AS token_symbol,
    token_address,
    decimals AS token_decimals
FROM
    base
