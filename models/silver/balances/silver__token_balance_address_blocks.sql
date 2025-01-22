{{ config(
    materialized = 'incremental',
    unique_key = ['address', 'contract_address'],
    cluster_by = ['address', 'contract_address'],
    tags = ['curated']
) }}

SELECT
    address,
    contract_address,
    MAX(block_number) AS max_block,
    MAX(_inserted_timestamp) AS _inserted_timestamp
FROM
    {{ ref('silver__token_balances') }}

WHERE
    _inserted_timestamp < (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            ethereum_dev.silver.token_balance_diffs
    )
{% if is_incremental() %}
    AND
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{this}}
    )
{% endif %}
GROUP BY
    address,
    contract_address