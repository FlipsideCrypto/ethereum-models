-- depends on: {{ ref('bronze__token_balances') }}
{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    cluster_by = ['_inserted_timestamp::date', 'block_timestamp::date'],
    tags = ['balances'],
    incremental_predicates = ["dynamic_range", "block_number"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

SELECT
    block_number,
    block_timestamp,
    address,
    contract_address,
    TRY_TO_NUMBER(
        PUBLIC.udf_hex_to_int(
            DATA :result :: STRING
        )
    ) AS balance,
    _inserted_timestamp,
    id
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
    AND DATA :result :: STRING <> '0x'
{% else %}
    {{ ref('bronze__fr_token_balances') }}
WHERE
    DATA :result :: STRING <> '0x'
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
