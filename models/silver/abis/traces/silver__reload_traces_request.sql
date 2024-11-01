{{ config (
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'tx_hash, trace_index',
    cluster_by = 'block_number'
) }}

{% set start_block = var(
    'start_block',
    1
) %}
{% set end_block = var(
    'end_block',
    1000000
) %}
WITH raw_traces AS (

    SELECT
        block_number,
        tx_hash,
        trace_index,
        from_address,
        to_address,
        TYPE,
        REGEXP_REPLACE(
            identifier,
            '[A-Z]+_',
            ''
        ) AS trace_address,
        sub_traces,
        CASE
            WHEN sub_traces > 0
            AND trace_address = 'ORIGIN' THEN 'ORIGIN'
            WHEN sub_traces > 0
            AND trace_address != 'ORIGIN' THEN trace_address || '_'
            ELSE NULL
        END AS parent_of,
        -- this adds an underscore
        IFF(REGEXP_REPLACE(trace_address, '.$', '') = '', 'ORIGIN', REGEXP_REPLACE(trace_address, '.$', '')) AS child_of,
        -- removes the last character
        input,
        output,
        _call_id
    FROM
        {{ ref('silver__traces') }}
        -- have to make this silver temporarily
        t
    WHERE
        (
            t.block_number BETWEEN {{ start_block }}
            AND {{ end_block }}
        ) -- and t.tx_hash = '0x38fef4fbf814f565433d538bee9cc8589b058aa5aede39d0be8854c65238562d'
        -- --and t.trace_index = 63
        -- AND t.block_number < (
        --     SELECT
        --         block_number
        --     FROM
        --         look_back
        -- )
        AND t.block_number IS NOT NULL -- AND _call_id NOT IN (
        --     SELECT
        --         _call_id
        --     FROM
        --         ETHEREUM_DEV.streamline.complete_decoded_traces
        --     WHERE
        --         (
        --             block_number BETWEEN 016944645
        --             AND 016980513
        --         )
        --         AND block_number < (
        --             SELECT
        --                 block_number
        --             FROM
        --                 look_back
        --         )
        -- )
),
PARENT AS (
    -- first takes trace calls where there are subtraces. These are the parent calls
    SELECT
        tx_hash,
        parent_of AS child_of,
        input
    FROM
        raw_traces
    WHERE
        sub_traces > 0
),
effective_contract AS (
    -- finds the effective implementation address for the parent trace
    SELECT
        tx_hash,
        TYPE AS child_type,
        to_address AS child_to_address,
        -- effective implementation
        child_of AS parent_of,
        input
    FROM
        raw_traces t
        INNER JOIN PARENT USING (
            tx_hash,
            child_of,
            input
        )
    WHERE
        TYPE = 'DELEGATECALL' qualify ROW_NUMBER() over (
            PARTITION BY t.tx_hash,
            t.child_of
            ORDER BY
                t.trace_index ASC
        ) = 1
),
final_traces AS (
    SELECT
        block_number,
        tx_hash,
        trace_index,
        from_address,
        to_address,
        TYPE,
        trace_address,
        sub_traces,
        parent_of,
        child_of,
        input,
        output,
        child_type,
        child_to_address,
        IFF(
            child_type = 'DELEGATECALL'
            AND child_to_address IS NOT NULL,
            child_to_address,
            to_address
        ) AS effective_contract_address,
        _call_id
    FROM
        raw_traces
        LEFT JOIN effective_contract USING (
            tx_hash,
            parent_of,
            input
        )
),
FINAL AS (
    SELECT
        t.block_number,
        t.tx_hash,
        t.trace_index,
        _call_id,
        f.abi AS abi,
        f.function_name,
        t.effective_contract_address AS abi_address,
        to_address,
        t.input,
        COALESCE(
            t.output,
            '0x'
        ) AS output
    FROM
        final_traces t
        LEFT JOIN {{ ref('silver__flat_function_abis') }}
        f
        ON t.effective_contract_address = f.contract_address
        AND LEFT(
            t.input,
            10
        ) = LEFT(
            f.function_signature,
            10
        )
    WHERE
        f.abi IS NOT NULL
),
final_2 AS (
    SELECT
        block_number,
        tx_hash,
        trace_index,
        to_address,
        abi_address,
        abi
    FROM
        FINAL
),
proposed AS (
    SELECT
        block_number,
        tx_hash,
        trace_index,
        ARRAY_AGG(
            i.value :name :: STRING
        ) AS input_proposed,
        ARRAY_AGG(
            o.value :name :: STRING
        ) AS output_proposed
    FROM
        final_2,
        LATERAL FLATTEN (
            input => abi :inputs
        ) i,
        LATERAL FLATTEN(
            input => abi :outputs
        ) o
    GROUP BY
        ALL
),
prod AS (
    SELECT
        block_number,
        tx_hash,
        trace_index,
        ARRAY_AGG(
            i.path
        ) AS input_prod,
        ARRAY_AGG(
            o.path
        ) AS output_prod
    FROM
        {{ ref('silver__decoded_traces') }},
        LATERAL FLATTEN (
            input => decoded_data :decoded_input_data
        ) i,
        LATERAL FLATTEN (
            input => decoded_data :decoded_output_data
        ) o
    WHERE
        block_number BETWEEN {{ start_block }}
        AND {{ end_block }}
        AND tx_status = 'SUCCESS'
        AND decoded_data :decoded_input_data :error IS NULL
        AND decoded_data :decoded_output_data :error IS NULL
    GROUP BY
        ALL
)
SELECT
    block_number,
    tx_hash,
    trace_index
FROM
    prod
    INNER JOIN proposed USING (
        tx_hash,
        trace_index,
        block_number
    )
WHERE
    array_sort(array_distinct(input_prod)) != array_sort(array_distinct(input_proposed))
    OR array_sort(array_distinct(output_prod)) != array_sort(array_distinct(output_proposed))
