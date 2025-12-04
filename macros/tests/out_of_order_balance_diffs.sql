{% test out_of_order_balance_diffs(model) %}
{% set model_name = model|string %}
{% set is_token_model = 'token_balance_diffs' in model_name %}
WITH ordered_balances AS (
    SELECT
        t.block_number,
        t.block_timestamp,
        t.address,
        {% if is_token_model %}
            t.contract_address,
        {% endif %}

        t.prev_bal_unadj,
        t.current_bal_unadj,
        t._inserted_timestamp,
        t.id,
        COALESCE(LAG(t.current_bal_unadj) ignore nulls over(PARTITION BY t.address
        {% if is_token_model %}
        , t.contract_address
        {% endif %}
    ORDER BY
        t.block_number ASC), 0) AS actual_previous_balance,
        CASE
            WHEN LAG(
                t.current_bal_unadj
            ) over(
                PARTITION BY t.address

                {% if is_token_model %},
                t.contract_address
                {% endif %}
                ORDER BY
                    t.block_number ASC
            ) IS NULL THEN TRUE
            ELSE FALSE
        END AS is_first_record
    FROM
        {{ model }}
        t
)
SELECT
    block_number,
    block_timestamp,
    address,
    {% if is_token_model %}
        contract_address,
    {% endif %}

    prev_bal_unadj,
    actual_previous_balance,
    current_bal_unadj,
    _inserted_timestamp,
    id,
    ABS(
        prev_bal_unadj - actual_previous_balance
    ) AS difference,
    is_first_record
FROM
    ordered_balances
WHERE
    prev_bal_unadj != actual_previous_balance
    AND actual_previous_balance IS NOT NULL
    AND is_first_record = FALSE
GROUP BY
    block_number,
    block_timestamp,
    address,
    {% if is_token_model %}
        contract_address,
    {% endif %}

    prev_bal_unadj,
    actual_previous_balance,
    current_bal_unadj,
    _inserted_timestamp,
    id,
    difference,
    is_first_record
HAVING
    COUNT(*) > 0 {% endtest %}
