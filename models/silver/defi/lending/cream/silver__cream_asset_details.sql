{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','lending','curated']
) }}

WITH contracts AS (

    SELECT
        *
    FROM
        {{ ref('core__dim_contracts') }}
),
log_pull AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        contract_address,
        c.name as token_name,
        c.symbol as token_symbol,
        c.decimals as token_decimals,
        l.modified_timestamp AS _inserted_timestamp,
        CONCAT(
            l.tx_hash,
            '-',
            l.event_index
        ) AS _log_id
    FROM
        {{ ref('core__fact_event_logs') }} l
    LEFT JOIN
        contracts c
    ON
        CONTRACT_ADDRESS = ADDRESS
    WHERE
        topics [0] :: STRING = '0x7ac369dbd14fa5ea3f473ed67cc9d598964a77501540ba6751eb0b3decf5870d'
        AND C.name LIKE '%Cream%'

{% if is_incremental() %}
AND l.modified_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
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
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                log_pull
        )
        AND CONCAT(TYPE, '_', trace_address) = 'STATICCALL_0_2'
),
contract_pull AS (
    SELECT
        l.tx_hash,
        l.block_number,
        l.block_timestamp,
        l.contract_address,
        token_name,
        token_symbol,
        token_decimals,
        CASE
            WHEN token_symbol = 'crETH' THEN LOWER('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2')
            ELSE t.underlying_asset
        END AS underlying_asset,
        l._inserted_timestamp,
        l._log_id
    FROM
        log_pull l
        LEFT JOIN traces_pull t
        ON l.contract_address = t.token_address qualify(ROW_NUMBER() over(PARTITION BY l.contract_address
    ORDER BY
        block_timestamp ASC)) = 1
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
