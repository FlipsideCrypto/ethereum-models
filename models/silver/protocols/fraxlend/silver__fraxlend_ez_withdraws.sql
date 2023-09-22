{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime'],
) }}

WITH log_join AS (

    SELECT
        tx_hash,
        block_timestamp,
        block_number,
        event_index,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics[1] :: STRING, 27, 42)) as caller,
        CONCAT('0x', SUBSTR(topics[2] :: STRING, 27, 42)) as receiver,
        CONCAT('0x', SUBSTR(topics[3] :: STRING, 27, 42)) as owner,
        utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            ) :: INTEGER / pow(10,decimals) as withdraw_amount,
        utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            ) :: INTEGER  / pow(10,decimals) AS withdraw_shares,
        withdraw_amount/NULLIF(withdraw_shares,0) as withdraw_share_price,
        f.frax_market_address,
        f.frax_market_symbol,
        f.underlying_asset,
        l._log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__fraxlend_ez_asset_details') }}
        f
        LEFT JOIN {{ ref('silver__logs') }}
        l
        ON f.frax_market_address = l.contract_address
    WHERE
        topics [0] = '0xfbde797d201c681b91056529119e0b02407c7bb96a4a2c75c01fc9667232c8db'

{% if is_incremental() %}
AND l._inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
prices AS (
  SELECT
    HOUR,
    token_address,
    price
  FROM
    {{ ref('core__fact_hourly_token_prices') }}
  WHERE
    token_address IN (
      SELECT
        DISTINCT underlying_asset
      FROM
        {{ ref('silver__fraxlend_ez_asset_details') }}
    )

{% if is_incremental() %}
AND HOUR >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
)
SELECT
    tx_hash,
    block_timestamp,
    block_number,
    event_index,
    caller,
    receiver,
    owner,
    withdraw_amount,
    ROUND(withdraw_amount * p.price,2) AS withdraw_amount_usd,
    withdraw_shares,
    withdraw_share_price,
    frax_market_address,
    frax_market_symbol,
    underlying_asset,
    _log_id,
    _inserted_timestamp
FROM
    log_join
 LEFT JOIN prices p
    ON underlying_asset = p.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p.hour
