-- depends on: {{ ref('bronze__streamline_eth_balances') }}
{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    cluster_by = ['block_timestamp::date','_inserted_timestamp::date'],
    incremental_predicates = ["dynamic_range", "block_number"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['non_realtime']
) }}

SELECT
    block_number,
    block_timestamp,
    COALESCE(
        VALUE :"ADDRESS" :: STRING,
        VALUE :"address" :: STRING
    ) AS address,
    TRY_TO_NUMBER(
        utils.udf_hex_to_int(
            DATA :result :: STRING
        )
    ) AS balance,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number', 'address']
    ) }} AS id,
    id AS eth_balances_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
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

qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
