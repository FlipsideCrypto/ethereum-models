{{ config (
    materialized = "view"
) }}

SELECT
    func_type,
    slot_number,
    state_id
FROM(
    SELECT
        func_type,
        slot_number,
        state_id
    FROM
        {{ ref("streamline__eth_validators") }}
    EXCEPT
    SELECT
        func_type,
        block_number AS slot_number,
        state_id
    FROM
        {{ ref("streamline__complete_validators") }}
)
WHERE func_type = 'validator_balances'


