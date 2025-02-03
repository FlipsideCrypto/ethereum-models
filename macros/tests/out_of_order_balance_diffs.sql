{% test out_of_order_balance_diffs(model) %}

WITH ordered_balances AS (
    SELECT 
        t.block_number,
        t.block_timestamp,
        t.address,
        t.prev_bal_unadj,
        t.current_bal_unadj,
        t._inserted_timestamp,
        t.id,
        COALESCE(LAG(t.current_bal_unadj) ignore nulls over(PARTITION BY t.address ORDER BY t.block_number ASC), 0) AS actual_previous_balance,
        CASE WHEN LAG(t.current_bal_unadj) over(PARTITION BY t.address ORDER BY t.block_number ASC) IS NULL THEN TRUE ELSE FALSE END AS is_first_record
    FROM 
        {{ model }} t
)

SELECT 
    block_number,
    block_timestamp,
    address,
    prev_bal_unadj,
    actual_previous_balance,
    current_bal_unadj,
    _inserted_timestamp,
    id,
    ABS(prev_bal_unadj - actual_previous_balance) as difference,
    is_first_record
FROM ordered_balances
WHERE prev_bal_unadj != actual_previous_balance
    AND actual_previous_balance IS NOT NULL 
    AND is_first_record = false
GROUP BY
    block_number,
    block_timestamp,
    address,
    prev_bal_unadj,
    actual_previous_balance,
    current_bal_unadj,
    _inserted_timestamp,
    id,
    difference,
    is_first_record
HAVING COUNT(*) > 0

{% endtest %}