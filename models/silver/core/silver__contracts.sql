{{ config(
    materialized = 'incremental',
    unique_key = 'address',
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(address, symbol, name), SUBSTRING(address, symbol, name)",
    tags = ['non_realtime']
) }}

WITH legacy AS (

    SELECT
        LOWER(address) :: STRING AS address,
        meta :symbol :: STRING AS symbol,
        meta :name :: STRING AS NAME,
        meta :decimals :: INTEGER AS decimals,
        meta AS contract_metadata,
        '2000-01-01' :: TIMESTAMP AS _inserted_timestamp
    FROM
        {{ source(
            'ethereum_bronze',
            'legacy_contracts'
        ) }}
    WHERE
        meta IS NOT NULL

{% if is_incremental() %}
AND 1 = 2
{% endif %}
),
streamline_reads AS (
    SELECT
        LOWER(
            A.contract_address
        ) :: STRING AS address,
        A.token_symbol :: STRING AS symbol,
        A.token_name :: STRING AS NAME,
        TRY_TO_NUMBER(
            A.token_decimals
        ) :: INTEGER AS decimals,
        contract_metadata,
        A._inserted_timestamp
    FROM
        {{ ref('silver__token_meta_reads') }} A
        LEFT JOIN legacy
        ON LOWER(contract_address) = LOWER(address)
{% if is_incremental() %}
WHERE
A._inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
all_records AS (
    SELECT
        address,
        symbol,
        NAME,
        decimals,
        contract_metadata,
        _inserted_timestamp
    FROM
        legacy
    UNION ALL
    SELECT
        address,
        symbol,
        NAME,
        decimals,
        contract_metadata,
        _inserted_timestamp
    FROM
        streamline_reads
)
SELECT
    
    address,
    symbol,
    NAME,
    decimals,
    contract_metadata,
    _inserted_timestamp,
  CASE 
        WHEN decimals IS NOT NULL AND symbol IS NOT NULL AND NAME IS NOT NULL THEN 7
        WHEN symbol IS NOT NULL AND decimals IS NOT NULL THEN 6
        WHEN NAME IS NOT NULL AND decimals IS NOT NULL THEN 5
        WHEN NAME IS NOT NULL AND symbol IS NOT NULL THEN 4
        WHEN decimals IS NOT NULL THEN  3
        WHEN symbol IS NOT NULL THEN  2
        WHEN name IS NOT NULL THEN  1
        ELSE 0
    END AS ranker,
    {{ dbt_utils.generate_surrogate_key(
        ['address']
    ) }} AS contracts_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_records qualify(ROW_NUMBER() over(PARTITION BY address
ORDER BY
    ranker DESC)) = 1
