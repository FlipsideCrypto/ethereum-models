{% test late_arriving_balance_diffs(model) %}

{{ config(severity = 'warn') }}

SELECT 
    block_number,
    block_timestamp,
    _inserted_timestamp,
    address,
    {% if model == 'silver__token_balance_diffs' %}
    contract_address,
    {% endif %}
    TIMEDIFF(hour, block_timestamp, _inserted_timestamp) as time_diff_hours
FROM {{ model }}
WHERE time_diff_hours > 25

{% endtest %}