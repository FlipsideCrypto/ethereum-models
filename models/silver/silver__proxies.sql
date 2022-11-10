{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"]
) }}

WITH base AS (

    SELECT
        tx_hash,
        block_number,
        contract_address,
        CONCAT('0x', SUBSTR(DATA, 27, 40)) AS proxy_address1,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS proxy_address2,
        CASE
            WHEN proxy_address1 = '0x' THEN proxy_address2
            ELSE proxy_address1
        END AS proxy_address,
        topics,
        DATA,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0xbc7cd75a20ee27fd9adebab32041f755214dbc6bffa90cc0225b39da2e5c2d3b'
        AND tx_status = 'SUCCESS'

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
    tx_hash,
    block_number,
    contract_address,
    proxy_address,
    _inserted_timestamp,
    {{ dbt_utils.surrogate_key(
        ['block_number', 'contract_address']
    ) }} AS id
FROM
    base qualify(ROW_NUMBER() over(PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
