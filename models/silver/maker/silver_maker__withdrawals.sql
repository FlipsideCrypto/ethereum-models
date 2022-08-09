{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = '_log_id', 
  cluster_by = ['block_timestamp::DATE', '_inserted_timestamp::DATE']
) }}

WITH get_withdrawals AS ( 
    SELECT 
        block_number, 
        block_timestamp, 
        tx_hash,
        tx_status, 
        origin_from_address AS withdrawer, 
        origin_to_address AS vault, 
        _inserted_timestamp, 
        _log_id
    FROM 
        {{ ref('silver__logs') }}
    WHERE 
        origin_to_address IN (
            SELECT 
                vault
            FROM 
                {{ ref('silver_maker__vault_creation') }} 
            )
        AND tx_hash NOT IN (
            SELECT 
                tx_hash
            FROM 
                {{ ref('silver__logs') }}
            WHERE 
                event_name IN ('Swap', 'FlashLoan', 'Repay', 'LogTrade', 'LogFlashLoan', 'Refund')
                OR contract_name LIKE '%TornadoCash%'
                OR contract_name IN ('GnosisToken', 'Proxy', 'DefisaverLogger', 'KyberNetwork', 'Exchange', 'AdminUpgradeabilityProxy')
            {% if is_incremental() %}
            AND
                _inserted_timestamp >= (
                    SELECT
                        MAX(_inserted_timestamp) 
                    FROM
                        {{ this }}
                )
            {% endif %}
        )

    {% if is_incremental() %}
    AND
        _inserted_timestamp >= (
            SELECT
                MAX(_inserted_timestamp) 
            FROM
                {{ this }}
        )
    {% endif %}

    qualify(ROW_NUMBER() over(PARTITION BY tx_hash
ORDER BY
    event_index ASC)) = 1
), 
transfer_amt AS (
    SELECT
        w.block_number, 
        w.block_timestamp, 
        w.tx_hash, 
        w.tx_status, 
        event_index, 
        withdrawer, 
        vault, 
        contract_address AS token_withdrawn,
        COALESCE(
            event_inputs :wad, 
            event_inputs :fee, 
            event_inputs :amount
         ) AS amount_withdrawn, 
         e._inserted_timestamp, 
         e._log_id
    FROM get_withdrawals w

    INNER JOIN {{ ref('silver__logs') }} e
    ON w.tx_hash = e.tx_hash

    WHERE e.event_name = 'Withdrawal'

    {% if is_incremental() %}
    AND e._inserted_timestamp >= (
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
    d.block_number, 
    d.block_timestamp, 
    d.tx_hash, 
    d.tx_status,
    d.event_index, 
    withdrawer, 
    vault, 
    token_withdrawn, 
    c.symbol, 
    amount_withdrawn, 
    COALESCE(
        c.decimals, 
        18
    ) AS decimals, 
    _inserted_timestamp, 
    _log_id
FROM transfer_amt d

LEFT OUTER JOIN {{ ref('core__dim_contracts') }} c
ON d.token_withdrawn = c.address