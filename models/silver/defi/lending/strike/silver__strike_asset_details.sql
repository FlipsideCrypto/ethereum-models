{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}
-- Pulls contract details for relevant c assets.  The case when handles cETH.
WITH log_pull AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        contract_address,
        _inserted_timestamp,
        _log_id
    FROM
        {{ ref('silver__logs') }}
        l
    WHERE
        topics [0] = '0x7ac369dbd14fa5ea3f473ed67cc9d598964a77501540ba6751eb0b3decf5870d'
        AND origin_from_address = lower('0x752dfb1C709EeA4621c8e95F48F3D0B6dde5d126')

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
                log_pull
        )
        AND identifier = 'STATICCALL_0_2'

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
        C.name,
        C.symbol,
        C.decimals,
        CASE
            WHEN l.contract_address = '0xbee9cf658702527b0acb2719c1faa29edc006a92' THEN LOWER('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2') --WETH
            ELSE t.underlying_asset
        END AS underlying_asset,
        l._inserted_timestamp,
        l._log_id
    FROM
        log_pull l
        LEFT JOIN traces_pull t
        ON l.contract_address = t.token_address
        LEFT JOIN contracts C
        ON C.address = l.contract_address qualify(ROW_NUMBER() over(PARTITION BY l.contract_address
    ORDER BY
        block_timestamp ASC)) = 1
)
SELECT
    l.block_number,
    l.block_timestamp,
    l.tx_hash,
    l.name AS itoken_name,
    l.symbol AS itoken_symbol,
    l.decimals AS itoken_decimals,
    l.contract_address AS itoken_address,
    C.name AS underlying_name,
    C.symbol AS underlying_symbol,
    C.decimals AS underlying_decimals,
    l.underlying_asset AS underlying_asset_address,
    l._inserted_timestamp,
    l._log_id
FROM
    contract_pull l
    LEFT JOIN contracts C
    ON C.address = l.underlying_asset
WHERE
    l.name IS NOT NULL
