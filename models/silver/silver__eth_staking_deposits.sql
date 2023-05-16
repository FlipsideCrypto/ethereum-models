{{ config(
    materialized = "incremental",
    unique_key = "_log_id",
    cluster_by = "block_timestamp::date"
) }}

WITH deposit_evt AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_from_address AS depositor,
        origin_to_address AS deposit_address,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT(
            '0x',
            segmented_data [6] :: STRING,
            SUBSTR(
                segmented_data [7] :: STRING,
                0,
                32
            )
        ) AS pubkey,
        CONCAT(
            '0x',
            segmented_data [9] :: STRING
        ) AS withdrawal_credentials,
        SUBSTR(
            withdrawal_credentials,
            1,
            4
        ) AS withdrawal_type,
        CONCAT('0x', SUBSTR(segmented_data [9] :: STRING, 25, 40)) AS withdrawal_address,
        CONCAT('0x', SUBSTR(segmented_data [11] :: STRING, 0, 16)) AS amount,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(SUBSTR(amount, 15, 2) || SUBSTR(amount, 13, 2) || SUBSTR(amount, 11, 2) || SUBSTR(amount, 9, 2) || SUBSTR(amount, 7, 2) || SUBSTR(amount, 5, 2) || SUBSTR(amount, 3, 2))
        ) / pow(
            10,
            9
        ) AS deposit_amount,
        CONCAT(
            '0x',
            segmented_data [13] :: STRING,
            segmented_data [14] :: STRING,
            segmented_data [15] :: STRING
        ) AS signature,
        CONCAT('0x', SUBSTR(segmented_data [17] :: STRING, 0, 16)) AS INDEX,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(SUBSTR(INDEX, 15, 2) || SUBSTR(INDEX, 13, 2) || SUBSTR(INDEX, 11, 2) || SUBSTR(INDEX, 9, 2) || SUBSTR(INDEX, 7, 2) || SUBSTR(INDEX, 5, 2) || SUBSTR(INDEX, 3, 2))
        ) AS deposit_index,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address = '0x00000000219ab540356cbb839cbe05303d7705fa' --BeaconDepositContract
        AND topics [0] :: STRING = '0x649bbc62d0e31342afea4e5cd82d4049e7e1ee912fc0889aa790803be39038c5' --DepositEvent
        AND block_number >= 11185311
),
FINAL AS (
    SELECT
        d.block_number,
        d.block_timestamp,
        d.tx_hash,
        d.event_index,
        deposit_amount,
        depositor,
        deposit_address,
        from_address AS platform_address,
        COALESCE(
            label,
            'other'
        ) AS platform,
        COALESCE(
            label_type,
            'other'
        ) AS platform_type,
        contract_address,
        COALESCE(
            address_name,
            'other'
        ) AS contract_name,
        COALESCE(
            label_subtype,
            'other'
        ) AS contract_type,
        pubkey,
        withdrawal_credentials,
        withdrawal_type,
        withdrawal_address,
        signature,
        deposit_index,
        _log_id,
        d._inserted_timestamp
    FROM
        deposit_evt d
        LEFT JOIN {{ ref('silver__traces') }}
        t
        ON d.block_number = t.block_number
        AND d.tx_hash = t.tx_hash
        AND d.deposit_amount :: INTEGER = t.eth_value :: INTEGER
        LEFT JOIN {{ ref('core__dim_labels') }} l
        ON from_address = l.address
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    deposit_amount,
    depositor,
    deposit_address,
    platform_address,
    platform,
    platform_type,
    contract_address,
    contract_name,
    contract_type,
    pubkey,
    withdrawal_credentials,
    withdrawal_type,
    withdrawal_address,
    signature,
    deposit_index,
    _log_id,
    _inserted_timestamp
FROM
    FINAL qualify(ROW_NUMBER() over (PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
