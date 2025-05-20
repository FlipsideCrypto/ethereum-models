{{ config (
    materialized = "view",
    post_hook = [fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_decode_traces_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"DECODED_TRACES", 
        "sql_limit" :"20000000",
        "producer_batch_size" :"500000",
        "worker_batch_size" :"20000",
        "sql_source" :"{{this.identifier}}" }
    ),
    fsc_utils.if_data_call_wait()],
    tags = ['streamline_decoded_traces_realtime']
) }}
WITH look_back AS (
        SELECT
            block_number
        FROM
            {{ ref("_24_hour_lookback") }}
    ),
    raw_traces AS (
        SELECT
            block_number,
            tx_hash,
            trace_index,
            from_address,
            to_address,
            TYPE,
            CONCAT(type,'_',trace_address) AS identifier,
            trace_address,
            sub_traces,
            CASE
                WHEN sub_traces > 0
                AND trace_address = 'ORIGIN' THEN 'ORIGIN'
                WHEN sub_traces > 0
                AND trace_address != 'ORIGIN' THEN trace_address || '_'
                ELSE NULL
            END AS parent_of,
            IFF(REGEXP_REPLACE(trace_address, '[0-9]+$', '') = '', 'ORIGIN', REGEXP_REPLACE(trace_address, '[0-9]+$', '')) AS child_of_raw, 
            iff(trace_address = 'ORIGIN', 'ORIGI', child_of_raw) as child_of, 
            input,
            output,
            concat_ws(
                '-',
                t.block_number,
                t.tx_position,
                identifier
            ) AS _call_id
        FROM
            {{ ref("core__fact_traces") }}
            t
        WHERE
                t.block_number >= (
                    SELECT
                        block_number
                    FROM
                        look_back
                )
               
                AND t.block_timestamp >= DATEADD('day', -2, CURRENT_DATE())
                AND _call_id NOT IN (
                    SELECT
                        _call_id
                    FROM
                        {{ ref("streamline__complete_decoded_traces") }}
                    WHERE
                        block_number >= (
                            SELECT
                                block_number
                            FROM
                                look_back
                        )
                        AND modified_timestamp >= DATEADD('day', -2, CURRENT_DATE())) 
                        
                        
                ),
                PARENT AS (
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
                    SELECT
                        tx_hash,
                        TYPE AS child_type,
                        to_address AS child_to_address,
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
                )
            SELECT
                t.block_number,
                t.tx_hash,
                t.trace_index,
                _call_id,
                f.abi AS abi,
                f.function_name,
                t.effective_contract_address AS abi_address,
                t.input,
                COALESCE(
                    t.output,
                    '0x'
                ) AS output
            FROM
                final_traces t
                LEFT JOIN {{ ref("silver__flat_function_abis") }}
                f
                ON t.effective_contract_address = f.contract_address
                AND LEFT(
                    t.input,
                    10
                ) = LEFT(
                    f.function_signature,
                    10
                )

                 where f.abi is not null 