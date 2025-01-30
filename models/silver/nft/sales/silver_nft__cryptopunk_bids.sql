{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH base AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index AS bid_event_index,
        utils.udf_hex_to_int(
            topics [1] :: STRING
        ) :: STRING AS bid_tokenid,
        '0x' || SUBSTR(
            topics [2] :: STRING,
            27
        ) AS bid_from_address,
        IFF(
            DATA = '0x0000000000000000000000000000000000000000000000000000000000000000',
            0,
            utils.udf_hex_to_int(
                DATA :: STRING
            ) :: INT
        ) AS bid_value,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        block_timestamp :: DATE >= '2017-06-20'
        AND contract_address = '0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb'
        AND topics [0] :: STRING IN (
            '0x5b859394fabae0c1ba88baffe67e751ab5248d2e879028b8c8d6897b0519f56a' -- PunkBidEntered
        )
        AND tx_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    *
FROM
    base
