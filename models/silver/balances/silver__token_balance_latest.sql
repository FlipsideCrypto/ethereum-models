{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    cluster_by = ['block_timestamp::date'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    tags = ['curated']
) }}

WITH base_table AS (

SELECT 
    address,
    contract_address,
    balance,
    block_number,
    block_timestamp,
    id,
    _inserted_timestamp,
    ROW_NUMBER() OVER (
        PARTITION BY address, contract_address 
        ORDER BY _inserted_timestamp DESC
    ) as rn
    FROM
        {{ ref('silver__token_balances') }}
WHERE
address in (
    '0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad',
    '0x000000fee13a103a10d593b9ae06b3e05f2e7e1c',
    '0x70bf6634ee8cb27d04478f184b9b8bb13e5f4710',
    '0x74de5d4fcbf63e00296fd95d33236b9794016631',
    '0x51c72848c68a965f66fa7a88855f9f7784502a7f',
    '0x1111111254eeb25477b68fb85ed929f73a960582',
    '0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43',
    '0x5141b82f5ffda4c6fe1e372978f1c5427640a190',
    '0xf89d7b9c864f589bbf53a82105107622b35eaa40',
    '0x28c6c06298d514db089934071355e5743bf21d60',
    '0x68d3a973e7272eb388022a5c6518d9b2a2e66fbf'
) ---10,554,436 
    QUALIFY rn = 1
)
SELECT
    f.*,
    id AS token_balance_latest_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base_table f
