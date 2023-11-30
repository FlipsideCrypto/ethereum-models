-- depends on: {{ ref('bronze__token_balances') }}
{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    cluster_by = ['_inserted_timestamp::date', 'block_timestamp::date'],
    incremental_predicates = ["dynamic_range", "block_number"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['non_realtime']
) }}

SELECT
    block_number,
    block_timestamp,
    address,
    contract_address,
    TRY_TO_NUMBER(
        CASE
            WHEN LENGTH(
                DATA :result :: STRING
            ) <= 4300
            AND DATA :result IS NOT NULL THEN utils.udf_hex_to_int(LEFT(DATA :result :: STRING, 66))
            ELSE NULL
        END
    ) AS balance,
    _inserted_timestamp,
    id,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number', 'address','contract_address']
    ) }} AS token_balances_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
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
