{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_index,
    topics [0] :: STRING AS topic_0,
    topics [1] :: STRING AS topic_1,
    topics [2] :: STRING AS topic_2,
    topics [3] :: STRING AS topic_3,
    'Transfer' AS event_name,
    DATA,
    regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
    CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS from_address,
    CONCAT('0x', SUBSTR(topic_2, 27, 40)) AS to_address,
    TRY_TO_NUMBER(
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        )
    ) AS lp_token_amount_unadj,
    (lp_token_amount_unadj / pow(10, 18)) :: FLOAT AS lp_token_amount_adj,
    CASE
        WHEN contract_address = '0x09d1d767edf8fa23a64c51fa559e0688e526812f' THEN 'OLAS-ETH'
        WHEN contract_address = '0xf9825a563222f9efc81e369311dadb13d68e60a4' THEN 'Bridged OLAS-WMATIC'
        WHEN contract_address = '0x36b203cb3086269f005a4b987772452243c0767f' THEN 'Bridged OLAS-WETH'
        WHEN contract_address = '0x27df632fd0dcf191c418c803801d521cd579f18e' THEN 'Bridged OLAS-WXDAI'
        WHEN contract_address = '0x2fd007a534eb7527b535a1df35aba6bd2a8b660f' THEN 'Bridged WETH-OLAS'
        WHEN contract_address = '0x9946d6fd1210d85ec613ca956f142d911c97a074' THEN 'Bridged OLAS-USDC'
        WHEN contract_address = '0x3685b8cc36b8df09ed9e81c1690100306bf23e04' THEN 'Bridged WSOL-OLAS'
    END AS lp_token_name,
    CASE
        WHEN contract_address = '0x09d1d767edf8fa23a64c51fa559e0688e526812f' THEN 'ethereum'
        WHEN contract_address = '0xf9825a563222f9efc81e369311dadb13d68e60a4' THEN 'polygon-pos'
        WHEN contract_address = '0x36b203cb3086269f005a4b987772452243c0767f' THEN 'arbitrum'
        WHEN contract_address = '0x27df632fd0dcf191c418c803801d521cd579f18e' THEN 'gnosis-chain'
        WHEN contract_address = '0x2fd007a534eb7527b535a1df35aba6bd2a8b660f' THEN 'optimism'
        WHEN contract_address = '0x9946d6fd1210d85ec613ca956f142d911c97a074' THEN 'base'
        WHEN contract_address = '0x3685b8cc36b8df09ed9e81c1690100306bf23e04' THEN 'solana'
    END AS source_chain,
    CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
    modified_timestamp AS _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS pol_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('core__fact_event_logs') }}
WHERE
    contract_address IN (
        '0x09d1d767edf8fa23a64c51fa559e0688e526812f',
        --OLAS-ETH (via Uniswap on Ethereum)
        '0xf9825a563222f9efc81e369311dadb13d68e60a4',
        --Bridged OLAS-WMATIC (via Balancer on Polygon POS)
        '0x36b203cb3086269f005a4b987772452243c0767f',
        --Bridged OLAS-WETH (via Balancer on Arbitrum)
        '0x27df632fd0dcf191c418c803801d521cd579f18e',
        --Bridged OLAS-WXDAI (via Balancer on Gnosis Chain)
        '0x2fd007a534eb7527b535a1df35aba6bd2a8b660f',
        --Bridged WETH-OLAS (via Balancer on Optimism)
        '0x9946d6fd1210d85ec613ca956f142d911c97a074',
        --Bridged OLAS-USDC (via Balancer on Base)
        '0x3685b8cc36b8df09ed9e81c1690100306bf23e04' --Bridged WSOL-OLAS (via Orca on Solana)
    ) --LP Tokens
    AND topic_0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' --Transfer
    AND (
        from_address = '0xa0da53447c0f6c4987964d8463da7e6628b30f82' --Treasury
        OR to_address = '0xa0da53447c0f6c4987964d8463da7e6628b30f82' --Treasury
    )
    AND from_address <> '0x0000000000000000000000000000000000000000'
    AND to_address <> '0x0000000000000000000000000000000000000000'
    AND tx_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}

