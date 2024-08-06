-- depends on: {{ ref('bronze__streamline_token_balances') }}
{{ config (
    materialized = "incremental",
    unique_key = "complete_token_balances_id",
    cluster_by = "ROUND(block_number, -3)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(complete_token_balances_id)",
    incremental_predicates = ["dynamic_range", "block_number"],
    tags = ['streamline_balances_complete']
) }}

SELECT
    COALESCE(
        VALUE :"BLOCK_NUMBER" :: INT,
        VALUE :"block_number" :: INT
    ) AS block_number,
    COALESCE(
        VALUE :"ADDRESS" :: STRING,
        VALUE :"address" :: STRING
    ) AS address,
    COALESCE(
        VALUE :"CONTRACT_ADDRESS" :: STRING,
        VALUE :"contract_address" :: STRING
    ) AS contract_address,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number', 'address', 'contract_address']
    ) }} AS complete_token_balances_id,
    _inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_token_balances') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__streamline_fr_token_balances') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY complete_token_balances_id
ORDER BY
    _inserted_timestamp DESC)) = 1
