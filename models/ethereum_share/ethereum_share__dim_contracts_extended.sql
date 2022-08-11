{{ config(
    materialized = 'incremental',
    unique_key = "contract_address",
    cluster_by = ['block_timestamp::date'],
    tags = ['share']
) }}

with dec_contracts as (
select contract_address
from {{ref('ethereum_share__fact_event_logs')}}
group by 1
)

SELECT
    system_created_at,
    block_number,
    block_timestamp,
    creator_address,
    c.contract_address,
    logic_address,
    token_convention,
    NAME,
    symbol,
    decimals,
    contract_metadata
FROM
    {{ ref('silver__contracts_extended') }} c
    join dec_contracts as dc on c.contract_address = dc.contract_address