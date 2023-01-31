{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_generic_jsonrpc_reads(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'beacon_blocks_receipts', 'route', 'eth_getBlockReceipts', 'producer_batch_size', 10000, 'producer_limit_size', 20000000, 'worker_batch_size', 100, 'producer_batch_chunks_size', 1000))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

SELECT
    slot_number AS block_number,
    'eth_getBlockReceipts' AS method,
    slot_number_hex AS params
FROM
    {{ ref("streamline__eth_pos_blocks_receipts") }}
EXCEPT
SELECT
    slot_number AS block_number,
    'eth_getBlockReceipts' AS method,
    REPLACE(
        concat_ws('', '0x', to_char(slot_number, 'XXXXXXXX')),
        ' ',
        ''
    ) AS params
FROM
    {{ ref("streamline__complete_eth_pos_blocks_receipts") }}
LIMIT 1000

    -- WITH last_3_days AS (
    --     SELECT
    --         block_number AS slot_number
    --     FROM
    --         {{ ref("_max_beacon_block_by_date") }}
    --         qualify ROW_NUMBER() over (
    --             ORDER BY
    --                 block_number DESC
    --         ) = 3
    -- )
    -- SELECT
    --     slot_number,
    --     state_id
    -- FROM
    --     {{ ref("streamline__eth_pos_blocks_receipts") }}
    -- WHERE
    --     (
    --         slot_number < (
    --             SELECT
    --                 slot_number
    --             FROM
    --                 last_3_days
    --         )
    --     )
    --     AND slot_number IS NOT NULL
    -- EXCEPT
    -- SELECT
    --     slot_number,
    --     state_id
    -- FROM
    --     {{ ref("streamline__complete_eth_pos_blocks_receipts") }}
    -- WHERE
    --     slot_number < (
    --         SELECT
    --             slot_number
    --         FROM
    --             last_3_days
    --     )
