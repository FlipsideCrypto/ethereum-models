{% test recent_decoded_logs_match(
    model
) %}
SELECT
    block_number,
    _log_id
FROM
    {{ model }}
    d
WHERE
    NOT EXISTS (
        SELECT
            1
        FROM
            {{ ref('silver__logs') }}
            l
        WHERE
            d.block_number = l.block_number
            AND d.tx_hash = l.tx_hash
            AND d.event_index = l.event_index
            AND d.contract_address = l.contract_address
            AND d.topics [0] :: STRING = l.topics [0] :: STRING
    ) 
{% endtest %}
