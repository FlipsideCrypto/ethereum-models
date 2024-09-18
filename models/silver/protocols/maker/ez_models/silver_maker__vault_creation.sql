{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    enabled = false,
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH logs_base AS (

    SELECT
        *,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CASE
            WHEN LEFT(
                topics [0] :: STRING,
                10
            ) = '0xd6be0bc1' THEN 'NewCDP'
            WHEN LEFT(
                topics [0] :: STRING,
                10
            ) = '0x6090dec5' THEN 'LogNote'
        END AS event_type
    FROM
        {{ ref('silver__logs') }}
    WHERE
        block_number > 8000000
        AND contract_address = '0x5ef30b9986345249bc32d8928b7ee64de9435e39'
        AND tx_status = 'SUCCESS'
        AND LEFT(
            topics [0] :: STRING,
            10
        ) IN (
            '0xd6be0bc1',
            '0x6090dec5'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
newcdps AS (
    SELECT
        tx_hash,
        event_index,
        topics,
        block_number,
        block_timestamp,
        _inserted_timestamp,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS user_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS owner_address,
        utils.udf_hex_to_int(
            topics [3] :: STRING
        ) AS vault_no,
        origin_to_address AS vault
    FROM
        logs_base
    WHERE
        event_type = 'NewCDP'
),
logNotes AS (
    SELECT
        tx_hash,
        event_index,
        topics,
        segmented_data,
        utils.udf_hex_to_string(REPLACE(topics [2] :: STRING, '0x', '')) AS ilk1,
        CASE
            WHEN RIGHT(
                ilk1,
                2
            ) :: STRING = '00' THEN utils.udf_hex_to_string(REPLACE(ilk1, '0x', ''))
        END AS ilk2,
        COALESCE(
            ilk2,
            ilk1
        ) AS ilk_l
    FROM
        logs_base
    WHERE
        event_type = 'LogNote'
),
FINAL AS (
    SELECT
        n.tx_hash,
        n.event_index,
        n.topics,
        n.block_number,
        n.block_timestamp,
        n._inserted_timestamp,
        n.user_address,
        n.owner_address,
        n.vault,
        n.vault_no :: INTEGER AS vault_no,
        SUBSTR(l.ilk_l, 0, POSITION('-', l.ilk_l) + 1) AS ilk
    FROM
        newcdps n
        JOIN logNotes l
        ON n.tx_hash = l.tx_hash
        AND n.event_index = l.event_index - 1
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    user_address,
    owner_address,
    vault,
    vault_no,
    ilk,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['vault_no']
    ) }} AS vault_creation_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
