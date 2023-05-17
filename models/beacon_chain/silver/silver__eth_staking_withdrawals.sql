{{ config(
    materialized = "incremental",
    unique_key = "_unique_key",
    cluster_by = "block_timestamp::date"
) }}

WITH withdrawal_blocks AS (

    SELECT
        block_number,
        block_timestamp,
        HASH AS block_hash,
        withdrawals AS withdrawals_data,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                VALUE :amount :: STRING
            )
        ) / pow(
            10,
            9
        ) AS withdrawal_amount,
        VALUE :address :: STRING AS withdrawal_address,
        withdrawals_root,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                VALUE :index :: STRING
            )
        ) AS withdrawal_index,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                VALUE :validatorIndex :: STRING
            )
        ) AS validator_index,
        {{ dbt_utils.generate_surrogate_key(
            ['block_number', 'withdrawal_address', 'withdrawal_index']
        ) }} AS _unique_key,
        _inserted_timestamp
    FROM
        {{ ref('silver__blocks') }},
        LATERAL FLATTEN(
            input => withdrawals
        )
    WHERE
        withdrawals IS NOT NULL
        AND block_number >= 17034869

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
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
    withdrawal_amount,
    withdrawal_address,
    withdrawals_root,
    withdrawal_index,
    validator_index,
    _unique_key,
    _inserted_timestamp
FROM
    withdrawal_blocks
