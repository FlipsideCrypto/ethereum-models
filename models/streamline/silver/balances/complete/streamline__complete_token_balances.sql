-- depends on: {{ ref('bronze__token_balances') }}
{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(id)",
    incremental_predicates = ["dynamic_range", "block_number"],
    tags = ['streamline_balances_complete']
) }}

SELECT
    block_number,
    address,
    contract_address,
    id,
    _inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze__token_balances') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__fr_token_balances') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
