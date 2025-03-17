{% macro v0_streamline_decoded_complete(
        model
    ) %}
SELECT
    block_number,
    file_name,
    id AS {% if model == 'decoded_logs' %}
        _log_id {% elif model == 'decoded_traces' %}
        _call_id
    {% endif %},
    {{ dbt_utils.generate_surrogate_key(
        ['id']
    ) }} AS {% if model == 'decoded_logs' %}
        complete_decoded_logs_id {% elif model == 'decoded_traces' %}
        complete_decoded_traces_id
    {% endif %},
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{% if model == 'decoded_logs' %}
    {{ ref('bronze__streamline_decoded_logs') }}

    {% elif model == 'decoded_traces' %}
    {{ ref('bronze__streamline_decoded_traces') }}
{% endif %}
WHERE
    TO_TIMESTAMP_NTZ(_inserted_timestamp) >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
    {% if model == 'decoded_logs' %}
        {{ ref('bronze__streamline_fr_decoded_logs') }}

        {% elif model == 'decoded_traces' %}
        {{ ref('bronze__streamline_fr_decoded_traces') }}
    {% endif %}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
{% endmacro %}