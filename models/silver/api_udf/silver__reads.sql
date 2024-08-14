-- depends_on: {{ ref('bronze__streamline_reads') }}
{{ config(
    materialized = 'incremental',
    unique_key = 'reads_id',
    cluster_by = ['_inserted_timestamp::date', 'function_signature'],
    incremental_predicates = ["dynamic_range", "block_number"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['non_realtime']
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
    DATA :result :: STRING AS read_output,
    COALESCE(
        VALUE :"FUNCTION_INPUT" :: STRING,
        VALUE :"function_input" :: STRING
    ) AS function_input,
    regexp_substr_all(SUBSTR(read_output, 3, len(read_output)), '.{64}') AS segmented_data,
    TO_TIMESTAMP_NTZ(_inserted_timestamp) AS _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address','block_number','function_signature','function_input']
    ) }} AS reads_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_reads') }}
WHERE
    TO_TIMESTAMP_NTZ(_inserted_timestamp) >= (
        SELECT
            DATEADD('hour', -3, MAX(_inserted_timestamp))
        FROM
            {{ this }})
        {% else %}
            {{ ref('bronze__streamline_fr_reads') }}
        {% endif %}

        qualify(ROW_NUMBER() over (PARTITION BY id
        ORDER BY
            _inserted_timestamp DESC)) = 1
