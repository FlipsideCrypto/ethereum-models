-- depends on: {{ ref('bronze__streamline_beacon_pending_deposits') }}
{{ config (
    materialized = "incremental",
    unique_key = "pending_deposits_id",
    cluster_by = "ROUND(request_slot_number, -3)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(pending_deposits_id)",
    incremental_predicates = ["dynamic_range", "request_slot_number"],
    tags = ['silver','beacon']
) }}

WITH base_pending_deposits AS (
    SELECT
        COALESCE(
            VALUE :"SLOT_NUMBER" :: INT,
            metadata :request :"slot_number" :: INT,
            PARSE_JSON(
                metadata :request :"slot_number"
            ) :: INT
        ) AS request_slot_number,
        try_to_number(data:amount::INTEGER) AS amount,
        data:pubkey::STRING AS pubkey,
        data:signature::STRING AS signature,
        data:slot::INTEGER AS submit_slot_number,
        data:withdrawal_credentials::STRING AS withdrawal_credentials,
        -- Use array_index from bronze layer (original position in API response array = queue position)
        -- array_index is 0-based, convert to 1-based to match BeaconScan format
        CASE
            WHEN COALESCE(
                VALUE :"array_index" :: INT,
                metadata :array_index :: INT,
                NULL
            ) IS NOT NULL THEN
                COALESCE(
                    VALUE :"array_index" :: INT,
                    metadata :array_index :: INT,
                    NULL
                ) + 1  -- Convert 0-based to 1-based
            ELSE NULL
        END AS queue_position,
        data,
        {{ dbt_utils.generate_surrogate_key(
            ['submit_slot_number', 'pubkey']
        ) }} AS deposit_id,
        {{ dbt_utils.generate_surrogate_key(
            ['request_slot_number', 'submit_slot_number', 'pubkey', 'signature']
        ) }} AS pending_deposits_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        _inserted_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_beacon_pending_deposits') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE(MAX(_inserted_timestamp), '1970-01-01' :: TIMESTAMP) _inserted_timestamp
        FROM
            {{ this }})
            AND (LEFT(
                DATA :error :: STRING,
                1
            ) <> 'F'
            OR DATA :error IS NULL
        )
        {% else %}
            {{ ref('bronze__streamline_fr_beacon_pending_deposits') }}
        WHERE
            (LEFT(
                DATA :error :: STRING,
                1
            ) <> 'F'
            OR DATA :error IS NULL
        )
        {% endif %}

    qualify(ROW_NUMBER() over (PARTITION BY pending_deposits_id
    ORDER BY
        _inserted_timestamp DESC)) = 1
),
queue_positioned AS (
    SELECT
        *,
        -- Count total deposits in queue at this request_slot_number
        SUM(CASE WHEN amount IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY request_slot_number) AS total_queue_size,
        -- Calculate total ETH amount ahead in queue (for ETH-based processing calculation)
        -- The deposit queue processes up to 256 ETH per epoch
        -- Sum ETH amounts of all deposits that come before this one in queue order
        -- Use queue_position (from array_index) for ordering, with fallback to submit_slot_number
        COALESCE(
            SUM(CASE WHEN amount IS NOT NULL THEN amount / pow(10, 9) ELSE 0 END) 
                OVER (
                    PARTITION BY request_slot_number 
                    ORDER BY COALESCE(queue_position, 999999) ASC, COALESCE(submit_slot_number, 0) ASC, pubkey ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ),
            0
        ) AS eth_ahead_at_request
    FROM
        base_pending_deposits
)
SELECT
    request_slot_number,
    amount,
    pubkey,
    signature,
    submit_slot_number,
    withdrawal_credentials,
    -- Calculate projected processing slot and epoch based on ETH amount in queue
    -- The deposit queue processes up to 256 ETH per epoch (Ethereum protocol specification)
    -- Formula based on ETH amounts:
    --   For deposits with submit_slot_number > 0:
    --     1. Calculate epochs between submission and request: request_epoch - submit_epoch
    --     2. Calculate ETH processed in that time: epochs_passed * 256 ETH
    --     3. ETH ahead at submission = ETH ahead at request + ETH processed
    --     4. Epochs needed to process all ETH ahead = CEIL(eth_ahead_at_submission / 256)
    --     5. Processing epoch = submit_epoch + epochs_needed
    --     6. Processing slot = processing_epoch * 32 (first slot of the epoch)
    --   For deposits with submit_slot_number = 0 or NULL:
    --     Use request_slot_number as reference point (assume they're in queue at request time)
    --     Epochs needed = CEIL(eth_ahead_at_request / 256)
    --     Processing epoch = request_epoch + epochs_needed
    --     Processing slot = processing_epoch * 32
    -- Calculate epoch first, then derive slot from epoch for precision
    CASE
        WHEN amount IS NOT NULL AND queue_position IS NOT NULL THEN
            CASE
                WHEN submit_slot_number > 0 AND request_slot_number >= submit_slot_number THEN
                    -- Deposit has valid submit_slot_number: calculate from submission time
                    FLOOR(submit_slot_number / 32) + 
                    CEIL(
                        (eth_ahead_at_request + 
                         (FLOOR(request_slot_number / 32) - FLOOR(submit_slot_number / 32)) * 256  -- ETH processed since submission (256 ETH per epoch)
                        ) / 256.0
                    )
                WHEN submit_slot_number = 0 OR submit_slot_number IS NULL THEN
                    -- Deposit with submit_slot_number = 0: calculate from request time
                    FLOOR(request_slot_number / 32) + 
                    CEIL(eth_ahead_at_request / 256.0)
                ELSE NULL
            END
        ELSE NULL
    END AS projected_processing_epoch,
    -- Calculate slot from epoch for precision (epoch * 32 = first slot of epoch)
    CASE
        WHEN amount IS NOT NULL AND queue_position IS NOT NULL THEN
            CASE
                WHEN submit_slot_number > 0 AND request_slot_number >= submit_slot_number THEN
                    -- Calculate slot from epoch for precision
                    (FLOOR(submit_slot_number / 32) + 
                     CEIL(
                         (eth_ahead_at_request + 
                          (FLOOR(request_slot_number / 32) - FLOOR(submit_slot_number / 32)) * 256
                         ) / 256.0
                     )) * 32
                WHEN submit_slot_number = 0 OR submit_slot_number IS NULL THEN
                    -- Calculate slot from epoch for precision
                    (FLOOR(request_slot_number / 32) + 
                     CEIL(eth_ahead_at_request / 256.0)) * 32
                ELSE NULL
            END
        ELSE NULL
    END AS projected_processing_slot,
    queue_position,
    total_queue_size,
    -- Projected processing timestamp based on projected_processing_epoch (when deposit is estimated to be processed)
    -- Calculate timestamp from epoch for precision: epoch * 32 slots * 12 seconds per slot
    CASE
        WHEN amount IS NOT NULL AND queue_position IS NOT NULL AND projected_processing_epoch IS NOT NULL THEN
            DATEADD(
                'seconds',
                projected_processing_epoch * 32 * 12,
                '2020-12-01T12:00:23Z' :: timestamp_ntz
            )
        ELSE NULL
    END AS projected_processing_timestamp,
    data,
    deposit_id,
    pending_deposits_id,
    inserted_timestamp,
    modified_timestamp,
    _inserted_timestamp,
    _invocation_id
FROM
    queue_positioned