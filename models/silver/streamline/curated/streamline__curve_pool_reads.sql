{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
) }}

WITH contract_deployments AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        from_address AS deployer_address,
        to_address AS contract_address
    FROM
        {{ ref('silver__traces') }}
        t
    WHERE
        -- these are the curve contract deployers, we may need to add here in the future
        from_address IN (
            '0xbabe61887f1de2713c6f97e567623453d3c79f67',
            '0x7eeac6cddbd1d0b8af061742d41877d7f707289a'
        )
        AND TYPE = 'CREATE'

{% if is_incremental() %}
AND (
    t.ingested_at >= COALESCE(
        (
            SELECT
                MAX(_inserted_timestamp)
            FROM
                {{ this }}
        ),
        '1900-01-01'
    )
    OR t.block_number IN (
        -- /*
        -- * If the block is not in the database, we need to ingest it.
        -- * This is to handle the case where the block is not in the database
        -- * because it was not loaded into the database.
        -- */
        SELECT
            block_number
        FROM
            {{ this }}
        WHERE
            block_number IS NULL
    )
)
{% endif %}
),
function_inputs AS (
    SELECT
        SEQ4() AS function_input
    FROM
        TABLE(GENERATOR(rowcount => 8))
),
FINAL AS (
    SELECT
        block_number,
        contract_address,
        'curve_pool_token_details' AS call_name,
        '0xc6610657' AS function_signature,
        (ROW_NUMBER() over (PARTITION BY contract_address
    ORDER BY
        block_number)) -1 AS function_input
    FROM
        contract_deployments
        LEFT JOIN function_inputs
)
SELECT
    {{ dbt_utils.surrogate_key(
        ['block_number', 'contract_address', 'function_signature', 'function_input']
    ) }} AS id,
    block_number,
    contract_address,
    call_name,
    function_signature,
    function_input,
    SYSDATE() AS _inserted_timestamp
FROM
    FINAL
