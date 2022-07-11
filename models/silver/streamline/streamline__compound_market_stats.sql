{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
) }}

WITH relevant_functions AS (

    SELECT
        *
    FROM
        {{ ref('core__dim_function_signatures') }}
    WHERE
        id IN (
            179,
            163256,
            165096,
            165099,
            163250,
            163249
        )
),
ctokens AS (
    SELECT
        address AS contract_address
    FROM
        {{ ref('core__dim_contracts') }}
    WHERE
        address IN (
            '0x70e36f6bf80a52b3b46b3af8e106cc0ed743e8e4',
            '0x4ddc2d193948926d02f9b1fe9e1daa0718270ed5',
            '0x5d3a536e4d6dbd6114cc1ead35777bab948e3643',
            '0x39aa39c021dfbae8fac545936693ac917d5e7563',
            '0x4b0181102a0112a2ef11abee5563bb4a3176c9d7',
            '0xb3319f5d18bc0d84dd1b4825dcde5d5f7266d407',
            '0x6c8c6b02e7b2be14d4fa6022dfd6d75921d90e4e',
            '0x12392f67bdf24fae0af363c24ac620a2f67dad86',
            '0x158079ee67fce2f58472a96584a73c7ab9ac95c1',
            '0xf5dce57282a584d2746faf1593d3121fcac444dc',
            '0xface851a4921ce59e912d19329929ce6da6eb0c7',
            '0x80a2ae356fc9ef4305676f7a3e2ed04e12c33946',
            '0x35a18000230da775cac24873d00ff85bccded550',
            '0x95b4ef2869ebd94beb4eee400a99824bf5dc325b',
            '0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9',
            '0xe65cdb6479bac1e22340e4e755fae7e509ecd06c',
            '0xc11b1268c1a384e55c48c2391d8d480264a3a7f4',
            '0xccf4429db6322d5c611ee964527d42e5d685dd6a'
        )
),
functions_join AS (
    SELECT
        contract_address,
        bytes_signature
    FROM
        ctokens
        JOIN relevant_functions
),
block_range AS (
    SELECT
        block_number
    FROM
        {{ ref('silver__blocks') }}
    WHERE
        block_number > 7500000

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
)
SELECT
    block_number,
    contract_address,
    bytes_signature AS function_signature,
    'compound_market_stats' AS call_name,
    0 AS function_input,
    SYSDATE() AS _inserted_timestamp,
    concat_ws(
        '-',
        block_number,
        contract_address,
        function_signature,
        function_input
    ) AS id
FROM
    block_range
    JOIN functions_join
