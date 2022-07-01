{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
    post_hook = "call {{this.schema}}.sp_get_{{this.identifier}}()"
) }}

WITH block_by_date AS (

    SELECT
        block_date,
        block_number
    FROM
        {{ ref("max_block_by_date") }}
    WHERE
        block_number > 15000000
),
transfers AS (
    SELECT
        _block_date,
        contract_address,
        address
    FROM
        {{ ref("token_transfers") }}
),
pending AS (
    SELECT
        b.block_number,
        t.address,
        t.contract_address
    FROM
        transfers t
        INNER JOIN block_by_date b
        ON b.block_date = t._block_date
    WHERE
        b.block_number > 15000000
    EXCEPT
    SELECT
        block_number,
        address,
        contract_address
    FROM
        {{ ref("streamline__complete_token_balances") }}
)
SELECT
    {{ dbt_utils.surrogate_key(
        ['block_number', 'contract_address', 'address']
    ) }} AS id,
    block_number,
    address,
    contract_address,
    SYSDATE() AS _inserted_timestamp
FROM
    pending
