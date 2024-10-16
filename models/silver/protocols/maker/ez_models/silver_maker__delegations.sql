{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    enabled = false,
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH txs_base AS (

    SELECT
        tx_hash
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address = '0x0a3f6849f78076aefadf113f5bed87720274ddc0' -- MakerDAO general governance contract

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
),
delegate_actions AS (
    SELECT
        tx_hash,
        block_timestamp,
        block_number,
        _log_id,
        topics,
        DATA,
        origin_from_address,
        origin_to_address,
        _inserted_timestamp,
        contract_address,
        CASE
            WHEN topics [0] :: STRING = '0x625fed9875dada8643f2418b838ae0bc78d9a148a18eee4ee1979ff0f3f5d427' THEN 'delegate'
            WHEN topics [0] :: STRING = '0xce6c5af8fd109993cb40da4d5dc9e4dd8e61bc2e48f1e3901472141e4f56f293' THEN 'undelegate'
        END AS tx_event,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS delegate,
        utils.udf_hex_to_int(
            DATA :: STRING
        ) :: INT AS amount_delegated_unadjusted,
        amount_delegated_unadjusted / pow(
            10,
            18
        ) AS amount_delegated,
        tx_status,
        event_index
    FROM
        {{ ref('silver__logs') }}
    WHERE
        tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                txs_base
        )
        AND topics [0] :: STRING IN (
            '0x625fed9875dada8643f2418b838ae0bc78d9a148a18eee4ee1979ff0f3f5d427',
            --lock
            '0xce6c5af8fd109993cb40da4d5dc9e4dd8e61bc2e48f1e3901472141e4f56f293'
        ) --free

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    tx_status,
    origin_from_address,
    contract_address,
    tx_event,
    delegate,
    amount_delegated_unadjusted,
    amount_delegated,
    _log_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    ) }} AS delegations_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    delegate_actions qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
