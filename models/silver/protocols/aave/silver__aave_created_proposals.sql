{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    tags = ['non_realtime']
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
    _log_id
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
