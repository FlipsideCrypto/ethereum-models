{{ config(
    materialized = "incremental",
    unique_key = "_unique_key",
    cluster_by = "block_timestamp::date",
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(block_hash,withdrawal_address,withdrawals_root), SUBSTRING(withdrawal_address)",
    tags = ['beacon']
) }}

WITH withdrawal_blocks AS (

    SELECT
        block_number,
        block_timestamp,
        HASH AS block_hash,
        withdrawals AS withdrawals_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                VALUE :amount :: STRING
            )
        ) / pow(
            10,
            9
        ) AS withdrawal_amount,
        VALUE :address :: STRING AS withdrawal_address,
        withdrawals_root,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                VALUE :index :: STRING
            )
        ) AS withdrawal_index,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                VALUE :validatorIndex :: STRING
            )
        ) AS validator_index,
        {{ dbt_utils.generate_surrogate_key(
            ['block_number', 'withdrawal_address', 'withdrawal_index']
        ) }} AS _unique_key,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_blocks') }},
        LATERAL FLATTEN(
            input => withdrawals
        )
    WHERE
        withdrawals IS NOT NULL
        AND block_number >= 17034869

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '24 hours'
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    block_hash,
    withdrawals_data,
    b.withdrawal_amount,
    withdrawal_address,
    withdrawals_root,
    withdrawal_index,
    b.validator_index,
    slot_number,
    epoch_number,
    _unique_key,
    b._inserted_timestamp,
    _unique_key AS eth_staking_withdrawals_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    withdrawal_blocks b
    LEFT JOIN {{ ref('silver__beacon_withdrawals') }}
    s
    ON b.validator_index = s.validator_index
    AND b.withdrawal_index = s.index
    AND b.withdrawal_address = s.address
