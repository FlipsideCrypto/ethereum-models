-- depends_on: {{ ref('bronze__streamline_reads') }}
{{ config (
    materialized = "incremental",
    unique_key = "complete_reads_id",
    cluster_by = "ROUND(block_number, -3)",
    incremental_predicates = ["dynamic_range", "block_number"],
    merge_update_columns = ["complete_reads_id"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(complete_reads_id)",
    tags = ['streamline_reads_complete']
) }}

SELECT
    COALESCE(
        VALUE :"CONTRACT_ADDRESS" :: STRING,
        VALUE :"contract_address" :: STRING
    ) AS contract_address,
    COALESCE(
        VALUE :"BLOCK_NUMBER" :: INT,
        VALUE :"block_number" :: INT
    ) AS block_number,
    COALESCE(
        VALUE :"FUNCTION_SIGNATURE" :: STRING,
        VALUE :"function_signature" :: STRING
    ) AS function_signature,
    COALESCE(
        VALUE :"CALL_NAME" :: STRING,
        VALUE :"call_name" :: STRING
    ) AS call_name,
    COALESCE(
        VALUE :"FUNCTION_INPUT" :: STRING,
        VALUE :"function_input" :: STRING
    ) AS function_input,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address', 'function_signature', 'call_name', 'function_input', 'block_number']
    ) }} AS complete_reads_id,
    TO_TIMESTAMP_LTZ(_inserted_timestamp) AS _inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_reads') }}
WHERE
    TO_TIMESTAMP_LTZ(_inserted_timestamp) >= (
        SELECT
            DATEADD('hour', -3, MAX(_inserted_timestamp))
        FROM
            {{ this }})
        {% else %}
            {{ ref('bronze__streamline_fr_reads') }}
        {% endif %}

        qualify(ROW_NUMBER() over (PARTITION BY complete_reads_id
        ORDER BY
            _inserted_timestamp DESC)) = 1
