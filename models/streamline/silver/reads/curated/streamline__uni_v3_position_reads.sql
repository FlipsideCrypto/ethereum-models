{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
    tags = ['streamline_reads_curated']
) }}
-- this model looks at the position function for uni v3 when there has been a liquidity action
-- 0x99fbab88
WITH liquidity_actions AS (

    SELECT
        block_number,
        contract_address,
        udf_hex_to_int(
            topics [1] :: STRING
        ) AS nf_position_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address = '0xc36442b4a4522e871399cd717abdd847ab11fe88'
        AND topics [0] :: STRING IN (
            '0x3067048beee31b25b2f1681f88dac838c8bba36af25bfb2b7cf7473a5847e35f',
            '0x26f6a048ee9138f2c0ce266f322cb99228e8d619ae2bff30c67f8dcf9d2377b4',
            '0x40d0efd1a53d60ecbf40971b9daf7dc90178c3aadc7aab1765632738fa8b8f01'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
),
FINAL AS (
    SELECT
        block_number,
        '0xc36442b4a4522e871399cd717abdd847ab11fe88' AS contract_address,
        -- proxy 0xc36442b4a4522e871399cd717abdd847ab11fe88
        '0x99fbab88' AS function_signature,
        TRIM(
            to_char(
                nf_position_id :: INTEGER,
                'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
            )
        ) AS function_input,
        _inserted_timestamp
    FROM
        liquidity_actions
)
SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['block_number', 'contract_address', 'function_signature', 'function_input']
    ) }} AS id,
    function_input,
    function_signature,
    block_number,
    contract_address,
    'uni_v3_position_reads' AS call_name,
    _inserted_timestamp
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
