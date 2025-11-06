{{ config(
    materialized = "incremental",
    unique_key = "ez_deposits_id",
    cluster_by = "block_timestamp::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(pubkey, depositor)",
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
    slot_number >= 11649077
    AND slot_number >= (
        SELECT
            MIN(slot_number)
        FROM
            {{ this }}
        WHERE
            slot_number >= 11649077
            AND (
                NOT processed
                OR block_number IS NULL
            )
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
        {# {% if is_incremental() %}
    WHERE
        processed_slot >= (
            SELECT
                MAX(processed_slot) - 7200
            FROM
                {{ this }}
        )
    {% endif %}

    #}
    qualify ROW_NUMBER() over (
        PARTITION BY submit_slot_number,
        pubkey
        ORDER BY
            processed_slot DESC
    ) = 1
),
eth_deposits AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        deposit_amount,
        depositor,
        deposit_address,
        platform_address,
        contract_address,
        pubkey,
        withdrawal_credentials,
        withdrawal_type,
        withdrawal_address,
        signature,
        deposit_index,
        eth_staking_deposits_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__eth_staking_deposits') }}

{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
)

{% if is_incremental() %},
processed_heal AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        deposit_amount,
        depositor,
        deposit_address,
        platform_address,
        contract_address,
        pubkey,
        withdrawal_credentials,
        withdrawal_type,
        withdrawal_address,
        signature,
        deposit_index,
        ez_deposits_id AS eth_staking_deposits_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ this }}
    WHERE
        block_number >= 22431132 -- first block for pending deposits
        AND (
            NOT processed
            OR slot_number IS NULL
        )
)
{% endif %},
combined_deposits AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        deposit_amount,
        depositor,
        deposit_address,
        platform_address,
        contract_address,
        pubkey,
        withdrawal_credentials,
        withdrawal_type,
        withdrawal_address,
        signature,
        deposit_index,
        eth_staking_deposits_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        eth_deposits

{% if is_incremental() %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    deposit_amount,
    depositor,
    deposit_address,
    platform_address,
    contract_address,
    pubkey,
    withdrawal_credentials,
    withdrawal_type,
    withdrawal_address,
    signature,
    deposit_index,
    eth_staking_deposits_id,
    inserted_timestamp,
    modified_timestamp
FROM
    processed_heal
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
    combined_deposits d
    LEFT JOIN blocks USING (block_number)
    LEFT JOIN confirmed_deposits USING (
        slot_number,
        pubkey
    )
    LEFT JOIN {{ ref('core__dim_labels') }}
    l
    ON d.platform_address = l.address qualify ROW_NUMBER() over (
        PARTITION BY ez_deposits_id
        ORDER BY
            d.modified_timestamp DESC
    ) = 1
