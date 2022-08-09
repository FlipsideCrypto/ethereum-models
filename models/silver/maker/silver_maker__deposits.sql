{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = '_log_id', 
  cluster_by = ['block_timestamp::DATE', '_inserted_timestamp::DATE']
) }}

WITH get_deposits AS ( 
    SELECT 
        block_number, 
        block_timestamp, 
        tx_hash,
        tx_status, 
        origin_from_address AS depositor, 
        origin_to_address AS vault, 
        _inserted_timestamp, 
        _log_id
    FROM 
        {{ ref('silver__logs') }} l
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
                OR contract_name IN ('GnosisToken', 'Proxy', 'DefisaverLogger', 'KyberNetwork', 'Exchange')
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
        d.block_number, 
        d.block_timestamp, 
        d.tx_hash,
        d.tx_status, 
        e.event_index, 
        depositor, 
        vault, 
        contract_address AS token_deposited, 
        COALESCE(
            event_inputs :value, 
            event_inputs :amount, 
            event_inputs :_amount, 
            event_inputs :undelyingDeposited
        ) AS amount_deposited, 
        e._inserted_timestamp, 
        e._log_id
    FROM get_deposits d

    INNER JOIN {{ ref('silver__logs') }} e
    ON d.tx_hash = e.tx_hash 

    WHERE e.event_name = 'Deposit'

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
    depositor, 
    vault, 
    token_deposited, 
    c.symbol, 
    amount_deposited :: NUMBER AS amount_deposited_unadjusted,
    COALESCE(
        c.decimals, 
        18
    ) AS decimals, 
    amount_deposited :: NUMBER / POW(10, COALESCE(c.decimals, 18)) AS amount_deposited, 
    _inserted_timestamp, 
    _log_id
FROM transfer_amt d

LEFT OUTER JOIN {{ ref('core__dim_contracts') }} c
ON d.token_deposited = c.address