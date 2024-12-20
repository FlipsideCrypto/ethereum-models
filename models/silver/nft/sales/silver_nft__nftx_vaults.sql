{{ config(
    materialized = 'incremental',
    unique_key = "vault_id",
    tags = ['curated']
) }}

WITH vaults AS (

    SELECT
        decoded_flat :assetAddress :: STRING AS nft_address,
        decoded_flat :vaultAddress :: STRING AS vault_address,
        decoded_flat :vaultId :: STRING AS vault_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        block_number >= 12676663
        AND contract_address = '0xbe86f647b167567525ccaafcd6f881f1ee558216'
        AND event_name = 'NewVault'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    *
FROM
    vaults
