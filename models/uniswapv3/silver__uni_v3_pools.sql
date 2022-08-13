{{ config(
    materialized = 'incremental',
    unique_key = 'pool_address',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH created_pools AS (

    SELECT
        block_number AS created_block,
        block_timestamp AS created_time,
        tx_hash AS created_tx_hash,
        LOWER(CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40))) AS token0_address,
        LOWER(CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))) AS token1_address,
        LOWER(CONCAT('0x', RIGHT(DATA, 40) :: STRING)) AS pool_address,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] = '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118'
        AND contract_address = '0x1f98431c8ad98523631ae4a59f267346ea31f984'

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
    created_block,
    created_time,
    created_tx_hash,
    token0_address,
    token1_address,
    pool_address,
    _inserted_timestamp
FROM
    created_pools qualify(ROW_NUMBER() over(PARTITION BY pool_address
ORDER BY
    _inserted_timestamp DESC)) = 1
