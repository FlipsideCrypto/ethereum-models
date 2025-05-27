{{ config(
    materialized = 'incremental',
    tags = ['silver','defi','lending','curated','asset_details'],
    unique_key = "ctoken_address"
) }}

WITH contracts_dim AS (
    SELECT
        address,
        name,
        symbol,
        decimals
    FROM
        {{ ref('core__dim_contracts') }}
),

ctoken_addresses AS (
    SELECT address FROM (
        VALUES
            (LOWER('0xe65cdB6479BaC1e22340E4E755fAE7E509EcD06c')), --cAAVE
            (LOWER('0x6C8c6b02E7b2BE14d4fA6022Dfd6d75921D90E4E')), --cBAT
            (LOWER('0x70e36f6BF80a52b3B46b3aF8e106CC0ed743E8e4')), --cCOMP
            (LOWER('0x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643')), --cDAI
            (LOWER('0x4Ddc2D193948926D02f9B1fE9e1daa0718270ED5')), --cETH
            (LOWER('0x7713DD9Ca933848F6819F38B8352D9A15EA73F67')), --cFEI
            (LOWER('0xFAce851a4921ce59e912d19329929CE6da6EB0c7')), --cLINK
            (LOWER('0x95b4eF2869eBD94BEb4eEE400a99824BF5DC325b')), --cMKR
            (LOWER('0x158079Ee67Fce2f58472A96584A73C7Ab9AC95c1')), --cREP
            (LOWER('0xF5DCe57282A584D2746FaF1593d3121Fcac444dC')), --cSAI
            (LOWER('0x4B0181102A0112A2ef11AbEE5563bb4a3176c9d7')), --cSUSHI
            (LOWER('0x12392F67bdf24faE0AF363c24aC620a2f67DAd86')), --cTUSD
            (LOWER('0x35A18000230DA775CAc24873d00Ff85BccdeD550')), --cUNI
            (LOWER('0x39AA39c021dfbaE8faC545936693aC917d5E7563')), --cUSDC
            (LOWER('0x041171993284df560249B57358F931D9eB7b925D')), --cUSDP
            (LOWER('0xf650C3d88D12dB855b8bf7D11Be6C55A4e07dCC9')), --cUSDT
            (LOWER('0xC11b1268C1A384e55C48c2391d8d480264A3A7F4')), --cWBTC
            (LOWER('0xccF4429DB6322D5C611ee964527D42E5d685DD6a')), --cWBTC2
            (LOWER('0x80a2AE356fc9ef4305676f7a3E2Ed04e12C33946')), --cYFI
            (LOWER('0xB3319f5D18Bc0D84dD1b4825Dcde5d5f7266d407')) --cZRX
    ) AS t(address)
),

comp_v2_logs AS (
    SELECT
        l.tx_hash,
        l.block_number,
        l.block_timestamp,
        l.contract_address,
        CASE 
            WHEN l.contract_address = '0xf5dce57282a584d2746faf1593d3121fcac444dc' THEN 'Compound SAI'
            ELSE c.name 
        END AS token_name,
        CASE 
            WHEN l.contract_address = '0xf5dce57282a584d2746faf1593d3121fcac444dc' THEN 'cSAI'
            ELSE c.symbol
        END AS token_symbol,
        CASE 
            WHEN l.contract_address = '0xf5dce57282a584d2746faf1593d3121fcac444dc' THEN 8
            ELSE c.decimals
        END AS token_decimals,
        l.modified_timestamp,
        CONCAT(l.tx_hash, '-', l.event_index) AS _log_id
    FROM
        {{ ref('core__fact_event_logs') }} l
        LEFT JOIN contracts_dim c ON l.contract_address = c.address
        JOIN ctoken_addresses ca ON l.contract_address = ca.address
    WHERE
        topics[0] :: STRING = '0x7ac369dbd14fa5ea3f473ed67cc9d598964a77501540ba6751eb0b3decf5870d'

    {% if is_incremental() %}
    AND l.modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '12 hours' FROM {{ this }}
    )
    AND l.modified_timestamp >= CURRENT_DATE() - INTERVAL '7 day'
    {% endif %}
),

traces_pull AS (
    SELECT
        from_address AS token_address,
        to_address AS underlying_asset
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        tx_hash IN (SELECT tx_hash FROM comp_v2_logs)
        AND CONCAT(TYPE, '_', trace_address) = 'STATICCALL_0_2'
),

contract_pull AS (
    SELECT
        l.tx_hash,
        l.block_number,
        l.block_timestamp,
        l.contract_address,
        l.token_name,
        l.token_symbol,
        l.token_decimals,
        CASE
            WHEN l.token_symbol = 'cETH' THEN LOWER('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2')
            ELSE t.underlying_asset
        END AS underlying_asset,
        l.modified_timestamp,
        l._log_id
    FROM
        comp_v2_logs l
        LEFT JOIN traces_pull t ON l.contract_address = t.token_address 
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY l.contract_address
        ORDER BY l.block_timestamp ASC
    ) = 1
),

comp_v2_join AS (
    SELECT
        l.contract_address AS ctoken_address,
        l.token_symbol AS ctoken_symbol,
        l.token_name AS ctoken_name,
        l.token_decimals AS ctoken_decimals,
        l.underlying_asset AS underlying_address,
        u.name AS underlying_name,
        u.symbol AS underlying_symbol,
        u.decimals AS underlying_decimals,
        l.block_number AS created_block,
        'Compound V2' AS compound_version
    FROM
        contract_pull l
        LEFT JOIN contracts_dim u ON l.underlying_asset = u.address
    WHERE
        l.underlying_asset IS NOT NULL
        AND l.token_name IS NOT NULL
),

comp_v3_base AS (
    SELECT
        contract_address,
        block_number,
        live.udf_api(
            'POST',
            '{URL}',
            OBJECT_CONSTRUCT(
                'Content-Type', 'application/json',
                'fsc-quantum-state', 'livequery'
            ),
            utils.udf_json_rpc_call(
                'eth_call',
                [
                    {
                        'to': contract_address, 
                        'from': null, 
                        'data': RPAD('0xc55dae63', 64, '0')
                    }, 
                    utils.udf_int_to_hex(22541338) -- dummy block number
                ],
                concat_ws('-', contract_address, '0xc55dae63', 22541338)
            ),
            'Vault/prod/evm/quicknode/ethereum/mainnet'
        ) AS api_response
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topic_0 = '0xbc7cd75a20ee27fd9adebab32041f755214dbc6bffa90cc0225b39da2e5c2d3b'
        AND origin_from_address IN (
            LOWER('0x343715FA797B8e9fe48b9eFaB4b54f01CA860e78'),
            LOWER('0x2501713A67a3dEdde090E42759088A7eF37D4EAb')
        )
        
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '12 hours' FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
    {% endif %}

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY contract_address
        ORDER BY block_number ASC
    ) = 1
),

comp_v3_data AS (
    SELECT
        l.contract_address AS ctoken_address,
        c1.symbol AS ctoken_symbol,
        c1.name AS ctoken_name,
        c1.decimals AS ctoken_decimals,
        LOWER(
            CONCAT(
                '0x',
                SUBSTR(
                    l.api_response:data:result :: STRING,
                    -40
                )
            )
        ) AS underlying_address,
        c2.name AS underlying_name,
        c2.symbol AS underlying_symbol,
        c2.decimals AS underlying_decimals,
        l.block_number AS created_block,
        'Compound V3' AS compound_version
    FROM comp_v3_base l
    LEFT JOIN contracts_dim c1 ON l.contract_address = c1.address
    LEFT JOIN contracts_dim c2 ON LOWER(
        CONCAT(
            '0x',
            SUBSTR(
                l.api_response:data:result :: STRING,
                -40
            )
        )
    ) = c2.address
    WHERE c1.name IS NOT NULL
),

comp_union AS (
    SELECT
        ctoken_address,
        ctoken_symbol,
        ctoken_name,
        ctoken_decimals,
        underlying_address,
        underlying_name,
        underlying_symbol,
        underlying_decimals,
        created_block,
        compound_version
    FROM comp_v2_join
    
    UNION ALL
    
    SELECT
        ctoken_address,
        ctoken_symbol,
        ctoken_name,
        ctoken_decimals,
        underlying_address,
        underlying_name,
        underlying_symbol,
        underlying_decimals,
        created_block,
        compound_version
    FROM comp_v3_data
)

SELECT
    ctoken_address,
    ctoken_symbol,
    ctoken_name,
    ctoken_decimals,
    underlying_address as underlying_asset_address,
    underlying_name,
    underlying_symbol,
    underlying_decimals,
    created_block,
    compound_version,
    {{ dbt_utils.generate_surrogate_key(['ctoken_address']) }} AS comp_asset_details_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    comp_union 
QUALIFY ROW_NUMBER() OVER(
    PARTITION BY ctoken_address
    ORDER BY created_block DESC
) = 1
