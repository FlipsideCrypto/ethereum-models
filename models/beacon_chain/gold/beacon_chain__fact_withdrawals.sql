{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    slot_number,
    slot_timestamp,
    epoch_number,
    (withdrawal_amount / pow(10, 9)) :: FLOAT AS withdrawal_amount,
    address,
    INDEX,
    validator_index
FROM
    {{ ref('silver__beacon_withdrawals') }}
