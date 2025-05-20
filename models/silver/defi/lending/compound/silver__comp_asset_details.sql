{{ config(
    materialized = 'table',
    tags = ['static']
) }}
-- Pulls contract details for relevant c assets.  The case when handles cETH.
WITH base AS (

    SELECT
        address :: STRING AS ctoken_address,
        symbol :: STRING AS ctoken_symbol,
        NAME :: STRING AS ctoken_name,
        decimals :: INTEGER AS ctoken_decimals,
        CASE
            WHEN address = '0x4ddc2d193948926d02f9b1fe9e1daa0718270ed5' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE LOWER(
                contract_metadata :underlying :: STRING
            )
        END AS underlying_asset_address,
        contract_metadata
    FROM
        {{ ref('core__dim_contracts') }}
    WHERE
        address IN (
            --cAAVE	
            lower('0xe65cdB6479BaC1e22340E4E755fAE7E509EcD06c'),	
            --cBAT	
            lower('0x6C8c6b02E7b2BE14d4fA6022Dfd6d75921D90E4E'),	
            --cCOMP	
            lower('0x70e36f6BF80a52b3B46b3aF8e106CC0ed743E8e4'),	
            --cDAI	
            lower('0x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643'),	
            --cETH	
            lower('0x4Ddc2D193948926D02f9B1fE9e1daa0718270ED5'),	
            --cFEI	
            lower('0x7713DD9Ca933848F6819F38B8352D9A15EA73F67'),	
            --cLINK	
            lower('0xFAce851a4921ce59e912d19329929CE6da6EB0c7'),	
            --cMKR	
            lower('0x95b4eF2869eBD94BEb4eEE400a99824BF5DC325b'),	
            --cREP	
            lower('0x158079Ee67Fce2f58472A96584A73C7Ab9AC95c1'),	
            --cSAI	
            lower('0xF5DCe57282A584D2746FaF1593d3121Fcac444dC'),	
            --cSUSHI	
            lower('0x4B0181102A0112A2ef11AbEE5563bb4a3176c9d7'),	
            --cTUSD	
            lower('0x12392F67bdf24faE0AF363c24aC620a2f67DAd86'),	
            --cUNI	
            lower('0x35A18000230DA775CAc24873d00Ff85BccdeD550'),	
            --cUSDC	
            lower('0x39AA39c021dfbaE8faC545936693aC917d5E7563'),	
            --cUSDP	
            lower('0x041171993284df560249B57358F931D9eB7b925D'),	
            --cUSDT	
            lower('0xf650C3d88D12dB855b8bf7D11Be6C55A4e07dCC9'),	
            --cWBTC	
            lower('0xC11b1268C1A384e55C48c2391d8d480264A3A7F4'),	
            --cWBTC2	
            lower('0xccF4429DB6322D5C611ee964527D42E5d685DD6a'),	
            --cYFI	
            lower('0x80a2AE356fc9ef4305676f7a3E2Ed04e12C33946'),	
            --cZRX	
            lower('0xB3319f5D18Bc0D84dD1b4825Dcde5d5f7266d407')
        )
),
comp_union as (
    SELECT
        b.ctoken_address,
        b.ctoken_symbol,
        b.ctoken_name,
        b.ctoken_decimals,
        b.underlying_asset_address,
        b.contract_metadata AS ctoken_metadata,
        CASE
            WHEN b.underlying_asset_address = '0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2' THEN 'Maker'
            WHEN b.underlying_asset_address = '0x1985365e9f78359a9b6ad760e32412f4a445e862' THEN 'Reputation'
            WHEN b.underlying_asset_address = '0x89d24a6b4ccb1b6faa2625fe562bdd9a23260359' THEN 'SAI Stablecoin'
            ELSE C.name :: STRING
        END AS underlying_name,
        CASE
            WHEN b.underlying_asset_address = '0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2' THEN 'MKR'
            WHEN b.underlying_asset_address = '0x1985365e9f78359a9b6ad760e32412f4a445e862' THEN 'REP'
            WHEN b.underlying_asset_address = '0x89d24a6b4ccb1b6faa2625fe562bdd9a23260359' THEN 'SAI'
            ELSE C.symbol :: STRING
        END AS underlying_symbol,
        CASE
            WHEN b.underlying_asset_address = '0x1985365e9f78359a9b6ad760e32412f4a445e862' THEN 18
            ELSE C.decimals :: INTEGER
        END AS underlying_decimals,
        C.contract_metadata AS underlying_contract_metadata,
        CASE
            WHEN b.ctoken_address = '0x4ddc2d193948926d02f9b1fe9e1daa0718270ed5' THEN 7710758
            WHEN b.ctoken_address = '0xe65cdb6479bac1e22340e4e755fae7e509ecd06c' THEN 12848198
            WHEN b.ctoken_address = '0xccf4429db6322d5c611ee964527d42e5d685dd6a' THEN 12038653
            WHEN b.ctoken_address = '0xf5dce57282a584d2746faf1593d3121fcac444dc' THEN 7710752
            WHEN b.ctoken_address = '0x80a2ae356fc9ef4305676f7a3e2ed04e12c33946' THEN 12848198
            WHEN b.ctoken_address = '0x95b4ef2869ebd94beb4eee400a99824bf5dc325b' THEN 12836064
            WHEN b.ctoken_address = '0x158079ee67fce2f58472a96584a73c7ab9ac95c1' THEN 7710755
            WHEN b.ctoken_address = '0x5d3a536e4d6dbd6114cc1ead35777bab948e3643' THEN 8983575
            WHEN b.ctoken_address = '0xface851a4921ce59e912d19329929ce6da6eb0c7' THEN 12286030
            WHEN b.ctoken_address = '0x4b0181102a0112a2ef11abee5563bb4a3176c9d7' THEN 12848166
            WHEN b.ctoken_address = '0x6c8c6b02e7b2be14d4fa6022dfd6d75921d90e4e' THEN 7710735
            WHEN b.ctoken_address = '0xb3319f5d18bc0d84dd1b4825dcde5d5f7266d407' THEN 7710733
            WHEN b.ctoken_address = '0x39aa39c021dfbae8fac545936693ac917d5e7563' THEN 7710760
            WHEN b.ctoken_address = '0x35a18000230da775cac24873d00ff85bccded550' THEN 10921410
            WHEN b.ctoken_address = '0x12392f67bdf24fae0af363c24ac620a2f67dad86' THEN 11008385
            WHEN b.ctoken_address = '0xc11b1268c1a384e55c48c2391d8d480264a3a7f4' THEN 8163813
            WHEN b.ctoken_address = '0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9' THEN 9879363
            WHEN b.ctoken_address = '0x70e36f6bf80a52b3b46b3af8e106cc0ed743e8e4' THEN 10960099
            WHEN b.ctoken_address = '0x041171993284df560249b57358f931d9eb7b925d' THEN 13258119
            WHEN b.ctoken_address = '0x7713dd9ca933848f6819f38b8352d9a15ea73f67' THEN 13227624
        END AS created_block,
        'Compound V2' AS compound_version
    FROM
        base b
        LEFT JOIN {{ ref('core__dim_contracts') }} C
        ON b.underlying_asset_address = C.address
    UNION ALL
    SELECT
        '0xa17581a9e3356d9a858b789d68b4d866e593ae94' AS ctoken_address,
        'cWETHv3' AS ctoken_symbol,
        'Compound WETH' AS ctoken_name,
        18 AS ctoken_decimals,
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS underlying_asset_address,
        NULL AS ctoken_metadata,
        'Wrapped Ether' AS underlying_name,
        'WETH' AS underlying_symbol,
        18 AS underlying_decimals,
        NULL as underlying_contract_metadata,
        16400710 AS created_block,
        'Compound V3' AS compound_version
    UNION ALL
    SELECT
        '0xc3d688b66703497daa19211eedff47f25384cdc3' AS ctoken_address,
        'cUSDCv3' AS ctoken_symbol,
        'Compound USDC' AS ctoken_name,
        6 AS ctoken_decimals,
        '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' AS underlying_address,
        NULL AS ctoken_metadata,
        'USDC' AS underlying_name,
        'USDC' AS underlying_symbol,
        6 AS underlying_decimals,
        NULL as underlying_contract_metadata,
        15331586 AS created_blocK,
        'Compound V3' AS compound_version
)
SELECT 
    *,    
    {{ dbt_utils.generate_surrogate_key(
        ['ctoken_address']
    ) }} AS comp_asset_details_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    comp_union qualify(ROW_NUMBER() over(PARTITION BY ctoken_address
ORDER BY
    created_blocK DESC)) = 1
