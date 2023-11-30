{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    tags = ['curated','reorg']
) }}

SELECT
    tx_hash,
    block_number,
    contract_address,
    decoded_flat :id :: INT AS proposal_id,
    decoded_flat :targets AS targets,
    decoded_flat :creator :: STRING AS proposer,
    decoded_flat :endBlock :: INTEGER AS end_voting_period,
    decoded_flat :startBlock :: INTEGER AS start_voting_period,
    'Created' AS status,
    _inserted_timestamp,
    _log_id,    
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    ) }} AS aave_created_proposals_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__decoded_logs') }} A
WHERE
    event_name = 'ProposalCreated'
    AND contract_address = '0xec568fffba86c094cf06b22134b23074dfe2252c'
    AND block_number > 11400000

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
