{{ config (
    materialized = "ephemeral"
) }}

SELECT
    contract_address,
    MAX(block_number) AS block_number,
    COUNT(*) AS events
FROM
    {{ ref("silver__logs") }}
    l
    LEFT JOIN {{ ref("silver__verified_abis") }}
    v USING (contract_address)
WHERE
    l.block_timestamp >= CURRENT_DATE - INTERVAL '30 days' -- recent activity
    AND v.contract_address IS NULL -- no verified abi
    AND l.contract_address NOT IN (
        SELECT
            contract_address
        FROM
            {{ ref("streamline__complete_contract_abis") }}
        WHERE
            _inserted_timestamp >= CURRENT_DATE - INTERVAL '30 days' -- this wont let us retry the same contract within 30 days
    )
GROUP BY
    contract_address
ORDER BY
    events DESC
LIMIT
    500
