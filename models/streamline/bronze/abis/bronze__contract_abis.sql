{{ config (
    materialized = 'view'
) }}

WITH base AS (
    {{ v0_streamline_external_table_query(
        model = "contract_abis_v3",
        partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER)"
    ) }}
)
SELECT
    partition_key,
    VALUE :"CONTRACT_ADDRESS" :: STRING AS contract_address,
    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
    base
