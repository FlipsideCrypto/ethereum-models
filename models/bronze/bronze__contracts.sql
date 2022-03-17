{{ config(
    materialized = 'incremental',
    unique_key = 'contract_address'
) }}

WITH base AS (

    SELECT
        *
    FROM
        {{ source(
            'bronze',
            'prod_ethereum_sink_407559501'
        ) }}
    WHERE
        record_content :model :name :: STRING IN (
            'eth_contracts_model'
        )

{% if is_incremental() %}
AND (
    record_metadata :CreateTime :: INT / 1000
) :: TIMESTAMP :: DATE >= (
    SELECT
        DATEADD('day', -1, MAX(system_created_at :: DATE))
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    (
        record_metadata :CreateTime :: INT / 1000
    ) :: TIMESTAMP AS system_created_at,
    t.value :block_id :: STRING AS block_id,
    t.value :block_timestamp :: TIMESTAMP AS block_timestamp,
    t.value :creator_address :: STRING AS creator_address,
    COALESCE(
        t.value :contract_address :: STRING,
        t.value :address :: STRING
    ) AS contract_address,
    t.value :logic_address :: STRING AS logic_address,
    COALESCE(
        t.value :contract_meta :symbol :: STRING,
        t.value :meta :symbol :: STRING
    ) AS symbol,
    COALESCE(
        t.value :contract_meta :decimals :: INTEGER,
        t.value :meta :decimals :: INTEGER
    ) AS decimals,
    COALESCE(
        t.value :contract_meta :: OBJECT,
        t.value :meta :: OBJECT
    ) AS meta,
    t.value :name :: STRING AS NAME,
    COALESCE(
        t.value :token_convention :: STRING,
        t.value :"token-convention" :: STRING
    ) AS token_convention
FROM
    base,
    LATERAL FLATTEN(
        input => record_content :results
    ) t
WHERE
    COALESCE(
        t.value :contract_meta,
        t.value :meta
    ) IS NOT NULL
    AND CASE
        WHEN meta :decimals :: STRING IS NOT NULL
        AND len(
            meta :decimals :: STRING
        ) >= 3 THEN TRUE
        ELSE FALSE
    END = FALSE qualify(ROW_NUMBER() over(PARTITION BY LOWER(contract_address)
ORDER BY
    system_created_at DESC)) = 1
