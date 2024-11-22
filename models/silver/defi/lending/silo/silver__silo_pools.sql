{{ config(
    materialized = 'incremental',
    tags = ['curated']
) }}

WITH logs_pull AS (

    SELECT
        *,
        CASE
            WHEN contract_address = LOWER('0x4D919CEcfD4793c0D47866C8d0a02a0950737589') THEN CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))
            WHEN contract_address = LOWER('0x8e3953Ac829441a1c8752ec7534a5E85cB84E495') THEN CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40))
            ELSE NULL
        END AS tokens
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address IN (
            LOWER('0x4D919CEcfD4793c0D47866C8d0a02a0950737589'),
            LOWER('0x8e3953Ac829441a1c8752ec7534a5E85cB84E495')
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
contracts AS (
    SELECT
        *
    FROM
        {{ ref('silver__contracts') }}
    WHERE
        ADDRESS IN (
            SELECT
                tokens
            FROM
                logs_pull
        )
),
silo_pull AS (
    SELECT
        block_number AS silo_create_block,
        tx_hash,
        l.contract_address AS factory_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS silo_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS token_address,
        utils.udf_hex_to_int(
            SUBSTR(
                segmented_data [0] :: STRING,
                27,
                40
            )
        ) :: INTEGER AS version,
        l._inserted_timestamp,
        l._log_id
    FROM
        logs_pull l
    WHERE
        l.contract_address = LOWER('0x4D919CEcfD4793c0D47866C8d0a02a0950737589')
),
silo_collateral_token AS (
    SELECT
        tx_hash,
        CONCAT('0x', SUBSTR(topics [0] :: STRING, 27, 40)) AS topic_0,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS protocol_collateral_token_address,
        C.symbol,
        C.decimals
    FROM
        logs_pull l
        LEFT JOIN contracts C
        ON CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) = C.ADDRESS
    WHERE
        l.contract_address = LOWER('0x8e3953Ac829441a1c8752ec7534a5E85cB84E495')
        AND topics [0] :: STRING = '0xd97e9f840332422474cda9bb0976c87735b44cda62a3fe2a4e13e2e862671812'
),
silo_debt_token AS (
    SELECT
        tx_hash,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS protocol_debt_token_address,
        C.symbol,
        C.decimals
    FROM
        logs_pull l
        LEFT JOIN contracts C
        ON CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) = C.ADDRESS
    WHERE
        l.contract_address = LOWER('0x8e3953Ac829441a1c8752ec7534a5E85cB84E495')
        AND topics [0] :: STRING = '0x94f128ebf0749edb8bb9d165d016ce008a16bc82cbd40cc81ded2be79140d020'
)
SELECT
    silo_create_block,
    l.tx_hash AS creation_hash,
    factory_address,
    silo_address,
    l.token_address,
    version,
    C.name,
    C.symbol,
    C.decimals,
    ct.protocol_collateral_token_address,
    ct.symbol AS protocol_collateral_token_symbol,
    ct.decimals AS protocol_collateral_token_decimals,
    dt.protocol_debt_token_address,
    dt.symbol AS protocol_debt_token_symbol,
    dt.decimals AS protocol_debt_token_decimals,
    l._log_id,
    l._inserted_timestamp
FROM
    silo_pull l
    LEFT JOIN contracts C
    ON C.ADDRESS = l.token_address
    LEFT JOIN silo_collateral_token ct
    ON ct.tx_hash = l.tx_hash
    LEFT JOIN silo_debt_token dt
    ON dt.tx_hash = l.tx_hash
WHERE
    silo_address IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    l._inserted_timestamp DESC)) = 1
