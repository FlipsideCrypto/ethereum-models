{{ config (
    materialized = "incremental",
    unique_key = "confirmed_deposits_id",
    cluster_by = "ROUND(processed_slot, -3)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(pubkey)",
    incremental_predicates = ["dynamic_range", "processed_slot"],
    tags = ['silver','beacon']
) }}

WITH latest_pending AS (

    SELECT
        request_slot_number,
        submit_slot_number,
        pubkey,
        signature,
        withdrawal_credentials,
        deposit_id,
        amount
    FROM
        {{ ref('silver__pending_deposits') }}
    WHERE
        request_slot_number = (
            SELECT
                MAX(request_slot_number)
            FROM
                {{ ref('silver__pending_deposits') }}
        )
        AND submit_slot_number > 0 qualify ROW_NUMBER() over (
            PARTITION BY deposit_id
            ORDER BY
                request_slot_number DESC
        ) = 1
),
new_pending_deposits AS (
    SELECT
        request_slot_number,
        submit_slot_number,
        pubkey,
        signature,
        withdrawal_credentials,
        deposit_id,
        amount
    FROM
        {{ ref('silver__pending_deposits') }}
    WHERE
        submit_slot_number > 0

{% if is_incremental() %}
AND request_slot_number >= (
    SELECT
        MAX(processed_slot)
    FROM
        {{ this }}
)
{% endif %}
),
processed_deposits AS (
    SELECT
        MAX(request_slot_number) AS processed_slot,
        submit_slot_number,
        pubkey,
        signature,
        withdrawal_credentials,
        deposit_id,
        amount
    FROM
        new_pending_deposits A
        LEFT JOIN latest_pending b USING (deposit_id)
    WHERE
        b.deposit_id IS NULL
    GROUP BY
        ALL
)
SELECT
    processed_slot,
    submit_slot_number,
    pubkey,
    signature,
    withdrawal_credentials,
    deposit_id,
    amount,
    {{ dbt_utils.generate_surrogate_key(
        ['processed_slot', 'submit_slot_number', 'pubkey', 'signature', 'amount']
    ) }} AS confirmed_deposits_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    processed_deposits
