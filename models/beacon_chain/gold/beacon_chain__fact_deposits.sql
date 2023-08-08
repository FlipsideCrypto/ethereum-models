{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    slot_number,
    slot_timestamp,
    epoch_number,
    (deposit_amount / pow(10, 9)) :: FLOAT AS deposit_amount,
    pubkey,
    signature,
    withdrawal_credentials,
    proofs
FROM
    {{ ref('silver__beacon_deposits') }}
