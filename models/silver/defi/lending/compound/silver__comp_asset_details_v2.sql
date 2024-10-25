{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH compv3_pull AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        CONCAT('0x', SUBSTR(topics [1], 27, 40)) :: STRING AS ctoken_address,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
    WHERE
        topics [0] :: STRING = '0xcc826d20934cb90e9329d09ff55b4e43831c5bb3a3305fb536842ad49041e7d5'

{% if is_incremental() %}
AND l._inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
    )
{% endif %}
),
underlying_details AS (
    SELECT
        LEFT(
            input,
            10
        ) AS function_sig,
        decoded_data :decoded_input_data :cometProxy :: STRING AS ctoken_address,
        decoded_data :decoded_input_data :newConfiguration :baseToken :: STRING AS underlying_address,
        decoded_data :decoded_input_data,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_traces') }}
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                compv3_pull
        )
        AND function_sig = '0xc1252f90' 
    QUALIFY ROW_NUMBER() OVER(PARTITION BY ctoken_address
    ORDER BY
        trace_index) = 1
    UNION ALL
    SELECT
        LEFT(
            input,
            10
        ) AS function_sig,
        '0xc3d688b66703497daa19211eedff47f25384cdc3' AS ctoken_address,
        decoded_data :decoded_input_data :config :baseToken :: STRING AS underlying_address,
        decoded_data :decoded_input_data,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_traces') }}
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                compv3_pull
        )
        AND function_sig = '0x2d526439' 
    QUALIFY ROW_NUMBER() OVER(PARTITION BY ctoken_address
    ORDER BY
        trace_index) = 1
),
detail_join AS (
    SELECT
        p.block_number,
        p.block_timestamp,
        p.tx_hash,
        u.function_sig,
        p.ctoken_address,
        u.underlying_address,
        p._inserted_timestamp,
        p._log_id
    FROM
        compv3_pull p
        JOIN underlying_details u
        ON p.ctoken_address = u.ctoken_address
),
compv3_final AS (
SELECT
    dj.block_number as created_block,
    dj.tx_hash,
    dj.function_sig,
    dj.ctoken_address,
    CASE
        WHEN dj.ctoken_address = '0xc3d688b66703497daa19211eedff47f25384cdc3' THEN 'Compound USDC'
        ELSE C.name
    END AS NAME,
    CASE
        WHEN dj.ctoken_address = '0xc3d688b66703497daa19211eedff47f25384cdc3' THEN 'cUSDCv3'
        ELSE C.symbol
    END AS symbol,
    CASE
        WHEN dj.ctoken_address = '0xc3d688b66703497daa19211eedff47f25384cdc3' THEN 6
        ELSE C.decimals
    END AS decimals,
    dj.underlying_address,
    c2.name AS underlying_name,
    c2.symbol AS underlying_symbol,
    c2.decimals AS underlying_decimals,
    dj._inserted_timestamp,
    dj._log_id
FROM
    detail_join dj
    LEFT JOIN {{ ref('silver__contracts') }} C
    ON C.address = dj.ctoken_address
    LEFT JOIN {{ ref('silver__contracts') }} c2
    ON c2.address = dj.underlying_address 
QUALIFY ROW_NUMBER() OVER(PARTITION BY dj.ctoken_address
ORDER BY
    dj._inserted_timestamp) = 1
),
comp_v2_log_pull AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        contract_address,
        _inserted_timestamp,
        _log_id
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0x7ac369dbd14fa5ea3f473ed67cc9d598964a77501540ba6751eb0b3decf5870d'
        AND origin_from_address = LOWER('0x54A37d93E57c5DA659F508069Cf65A381b61E189')

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
traces_pull AS (
    SELECT
        from_address AS token_address,
        to_address AS underlying_asset
    FROM
        {{ ref('silver__traces') }}
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                comp_v2_log_pull
        )
        AND identifier = 'STATICCALL_0_2'
),
contracts AS (
    SELECT
        *
    FROM
        {{ ref('silver__contracts') }}
),
contract_pull AS (
    SELECT
        l.tx_hash,
        l.block_number,
        l.block_timestamp,
        l.contract_address,
        C.name as token_name,
        C.symbol as token_symbol,
        C.decimals as token_decimals,
        CASE 
            WHEN token_symbol = 'sETH' THEN LOWER('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2')
            ELSE t.underlying_asset
        END AS underlying_asset,
        l._inserted_timestamp,
        l._log_id
    FROM
        comp_v2_log_pull l
        LEFT JOIN traces_pull t
        ON l.contract_address = t.token_address
        LEFT JOIN contracts C
        ON C.address = l.contract_address 
    QUALIFY ROW_NUMBER() OVER(PARTITION BY l.contract_address
    ORDER BY
        block_timestamp ASC) = 1
)

SELECT
    l.tx_hash,
    l.block_number,
    l.block_timestamp,
    l.contract_address AS token_address,
    l.token_name,
    l.token_symbol,
    l.token_decimals,
    l.underlying_asset AS underlying_asset_address,
    C.name AS underlying_name,
    C.symbol AS underlying_symbol,
    C.decimals AS underlying_decimals,
    l._inserted_timestamp,
    l._log_id
FROM
    contract_pull l
    LEFT JOIN contracts C
    ON C.address = l.underlying_asset
WHERE
    underlying_asset IS NOT NULL
    AND l.token_name IS NOT NULL
UNION ALL 
SELECT
    dj.tx_hash,
    created_block,
    block_timestamp,
    ctoken_address AS token_address,
    name AS token_name,
    symbol AS token_symbol,
    decimals AS token_decimals,
    dj.underlying_address AS underlying_asset_address,
    underlying_name,
    underlying_symbol,
    underlying_decimals,
    _inserted_timestamp,
    _log_id
FROM
    compv3_final
