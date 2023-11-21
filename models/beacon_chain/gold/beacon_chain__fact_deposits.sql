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
    proofs,
    COALESCE (
       beacon_deposits_id,
        {{ dbt_utils.generate_surrogate_key(
             ['slot_number', 'signature', 'proofs']
        ) }}
    ) AS fact_deposits_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__beacon_deposits') }}
