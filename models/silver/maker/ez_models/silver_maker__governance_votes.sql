{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    tx_status,
    event_index,
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS voter,
    contract_address AS polling_contract,
    PUBLIC.udf_hex_to_int(
        topics [3] :: STRING
    ) :: INT AS vote_option,
    PUBLIC.udf_hex_to_int(
        topics [2] :: STRING
    ) :: INT AS proposal_id,
    _inserted_timestamp,
    _log_id
FROM
    {{ ref('silver__logs') }}
WHERE
    contract_address IN (
        '0xd3a9fe267852281a1e6307a1c37cdfd76d39b133',
        '0x47f18d6e82f623b810d912e115de4f5f805edc5d',
        '0xf9be8f0945acddeedaa64dfca5fe9629d0cf8e5d'
    )
    AND topics [0] :: STRING = '0xea66f58e474bc09f580000e81f31b334d171db387d0c6098ba47bd897741679b'

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