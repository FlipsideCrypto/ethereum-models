{{ config(
    materialized = "incremental",
    unique_key = "ez_deposits_id",
    cluster_by = "block_timestamp::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(ez_deposits_id)",
    tags = ['gold','beacon']
) }}

WITH blocks AS (

    SELECT
        slot_number,
        block_number
    FROM
        {{ ref('beacon_chain__fact_blocks') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp) - INTERVAL '24 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
confirmed_deposits AS (
    SELECT
        processed_slot,
        submit_slot_number AS slot_number,
        pubkey
    FROM
        {{ ref('silver__confirmed_deposits') }}

{% if is_incremental() %}
WHERE
    processed_slot >= (
        SELECT
            MAX(processed_slot) - 7200
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    slot_number,
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    deposit_amount,
    depositor,
    deposit_address,
    platform_address,
    COALESCE(
        label,
        'not labeled'
    ) AS platform,
    COALESCE(
        label_type,
        'not labeled'
    ) AS platform_category,
    COALESCE(
        label_subtype,
        'not labeled'
    ) AS platform_address_type,
    contract_address,
    pubkey,
    withdrawal_credentials,
    withdrawal_type,
    withdrawal_address,
    signature,
    deposit_index,
    IFF(
        processed_slot IS NULL,
        NULL,
        processed_slot
    ) AS processed_slot,
    CASE
        -- min slot number for pending slot deposits
        WHEN slot_number < 11649077
        OR slot_number IS NULL THEN TRUE
        WHEN slot_number >= 11649077
        AND processed_slot IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS processed,
    COALESCE (
        d.eth_staking_deposits_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'event_index']
        ) }}
    ) AS ez_deposits_id,
    GREATEST(
        COALESCE (
            d.inserted_timestamp,
            '2000-01-01'
        ),
        COALESCE (
            l.inserted_timestamp,
            '2000-01-01'
        )
    ) AS inserted_timestamp,
    GREATEST(
        COALESCE (
            d.modified_timestamp,
            '2000-01-01'
        ),
        COALESCE (
            l.modified_timestamp,
            '2000-01-01'
        )
    ) AS modified_timestamp
FROM
    {{ ref('silver__eth_staking_deposits') }}
    d
    LEFT JOIN blocks USING (block_number)
    LEFT JOIN confirmed_deposits USING (
        slot_number,
        pubkey
    )
    LEFT JOIN {{ ref('core__dim_labels') }}
    l
    ON d.platform_address = l.address

{% if is_incremental() %}
AND d.modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '24 hours'
    FROM
        {{ this }}
)
{% endif %}
