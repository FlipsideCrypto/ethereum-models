{{ config(
    materialized = 'incremental',
    sort = 'block_number',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'AAVE',
                'PURPOSE': 'DEFI'
            }
        }
    },
    persist_docs ={ "relation": true,
    "columns": true }
) }}

WITH base AS (

  SELECT
    block_number,
    block_timestamp,
    contract_address AS governance_contract,
    regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
    utils.udf_hex_to_int(
      segmented_data [0] :: STRING
    ) :: INTEGER AS proposal_id,
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS voter,
    CASE
      WHEN utils.udf_hex_to_int(
        segmented_data [1] :: STRING
      ) :: INTEGER = 1 THEN TRUE
      ELSE FALSE
    END AS support,
    utils.udf_hex_to_int(
      segmented_data [2] :: STRING
    ) AS voting_power,
    tx_hash,
    'ethereum' AS blockchain,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver__logs') }}
  WHERE
    contract_address = '0xec568fffba86c094cf06b22134b23074dfe2252c'
    AND topics [0] :: STRING = '0x0c611e7b6ae0de26f4772260e1bbdb5f58cbb7c275fe2de14671968d29add8d6'

{% if is_incremental() %}
AND _inserted_timestamp >= (
  SELECT
    MAX(
      _inserted_timestamp
    ) - INTERVAL '12 hours'
  FROM
    {{ this }}
)
{% endif %}
)
SELECT
  block_number,
  block_timestamp,
  governance_contract,
  proposal_id,
  support,
  voting_power,
  voter,
  tx_hash,
  'ethereum' AS blockchain,
  _log_id,
  _inserted_timestamp
FROM
  base
