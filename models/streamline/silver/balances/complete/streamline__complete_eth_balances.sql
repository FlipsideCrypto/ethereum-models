-- depends on: {{ ref('bronze__streamline_eth_balances') }}
{{ config (
    materialized = "incremental",
    unique_key = "complete_eth_balances_id",
    cluster_by = "ROUND(block_number, -3)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(complete_eth_balances_id)",
    incremental_predicates = ["dynamic_range", "block_number"],
    tags = ['streamline_balances_complete']
) }}

SELECT
    block_number,
    COALESCE(
        VALUE :"ADDRESS" :: STRING,
        VALUE :"address" :: STRING
    ) AS address,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number', 'address']
    ) }} AS complete_eth_balances_id,
    _inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_eth_balances') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__streamline_fr_eth_balances') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY complete_eth_balances_id
ORDER BY
    _inserted_timestamp DESC)) = 1
