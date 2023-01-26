{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['compound'],
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'COMPOUND',
                'PURPOSE': 'DEFI'
            }
        }
    }
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
            '0x6c8c6b02e7b2be14d4fa6022dfd6d75921d90e4e',
            -- cbat
            '0x70e36f6bf80a52b3b46b3af8e106cc0ed743e8e4',
            -- ccomp
            '0x5d3a536e4d6dbd6114cc1ead35777bab948e3643',
            -- cdai
            '0x4ddc2d193948926d02f9b1fe9e1daa0718270ed5',
            -- cETH
            '0x158079ee67fce2f58472a96584a73c7ab9ac95c1',
            -- cREP
            '0xf5dce57282a584d2746faf1593d3121fcac444dc',
            -- csai
            '0x35a18000230da775cac24873d00ff85bccded550',
            -- cuni
            '0x39aa39c021dfbae8fac545936693ac917d5e7563',
            -- cusdc
            '0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9',
            -- cusdt
            '0xc11b1268c1a384e55c48c2391d8d480264a3a7f4',
            -- cwbtc
            '0xccf4429db6322d5c611ee964527d42e5d685dd6a',
            -- cwbtc2
            '0xb3319f5d18bc0d84dd1b4825dcde5d5f7266d407',
            -- czrx
            '0xe65cdb6479bac1e22340e4e755fae7e509ecd06c',
            -- caave
            '0xface851a4921ce59e912d19329929ce6da6eb0c7',
            -- clink
            '0x95b4ef2869ebd94beb4eee400a99824bf5dc325b',
            -- cmkr
            '0x4b0181102a0112a2ef11abee5563bb4a3176c9d7',
            -- csushi
            '0x80a2ae356fc9ef4305676f7a3e2ed04e12c33946',
            -- cyfi
            '0x12392f67bdf24fae0af363c24ac620a2f67dad86'
        ) -- ctusd)
)
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
    END AS created_block
FROM
    base b
    LEFT JOIN {{ ref('core__dim_contracts') }} C
    ON b.underlying_asset_address = C.address
