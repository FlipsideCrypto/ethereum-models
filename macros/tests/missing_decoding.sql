{% test missing_decoded_logs(model) %}
SELECT
    l.block_number,
    l._log_id
FROM
    {{ ref('silver__logs') }}
    l
    LEFT JOIN {{ model }}
    d
    ON l.block_number = d.block_number
    AND l._log_id = d._log_id
WHERE
    l.contract_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' -- WETH
    AND l.topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' -- Transfer
    AND l.block_timestamp BETWEEN DATEADD('hour', -48, SYSDATE())
    AND DATEADD('hour', -6, SYSDATE())
    AND d._log_id IS NULL {% endtest %}
    {% test missing_decoded_traces(model) %}
SELECT
    t.block_number,
    t.tx_hash,
    t.trace_index
FROM
    {{ ref('core__fact_traces') }}
    t
    LEFT JOIN {{ model }}
    d
    USING (
        block_number,
        tx_hash,
        trace_index
    )
WHERE
    t.to_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' -- WETH
    AND LEFT(
        t.input,
        10
    ) = '0xa9059cbb' -- transfer(address,uint256)
    AND t.block_timestamp BETWEEN DATEADD('hour', -48, SYSDATE())
    AND DATEADD('hour', -6, SYSDATE())
    AND d._call_id IS NULL {% endtest %}
