{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['stale']
) }}

WITH raw_traces AS (

    SELECT
        *,
        decoded_data :function_name :: STRING AS function_name
    FROM
        {{ ref('silver__decoded_traces') }}
    WHERE
        block_timestamp :: DATE >= '2022-04-24'
        AND to_address = '0xb16c1342e617a5b6e4b631eb114483fdb289c0a4' -- sudoswap v1 pair factory
        AND trace_status = 'SUCCESS'
        AND TYPE = 'CALL'
        AND function_name IN (
            -- only 721 pairs
            'createPairETH',
            'createPairERC20'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
eth_pool AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        trace_index,
        to_address,
        decoded_data,
        function_name,
        decoded_data :decoded_input_data :_assetRecipient :: STRING AS asset_recipient,
        decoded_data :decoded_input_data :_bondingCurve :: STRING AS bonding_curve_address,
        decoded_data :decoded_input_data :_delta :: INT AS delta,
        decoded_data :decoded_input_data :_initialNFTIDs AS initial_nft_ids_array,
        NULL AS initial_token_balance,
        decoded_data :decoded_input_data :_spotPrice :: INT AS spot_price,
        decoded_data :decoded_input_data :_fee :: INT AS fee,
        decoded_data :decoded_input_data :_nft :: STRING AS nft_address,
        decoded_data :decoded_input_data :_poolType :: INT AS pool_type,
        decoded_data :decoded_output_data :pair :: STRING AS pool_address,
        'ETH' AS token_address,
        _inserted_timestamp
    FROM
        raw_traces
    WHERE
        function_name = 'createPairETH'
),
erc20_pool AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        trace_index,
        to_address,
        decoded_data,
        function_name,
        decoded_data :decoded_input_data :params :assetRecipient :: STRING AS asset_recipient,
        decoded_data :decoded_input_data :params :bondingCurve :: STRING AS bonding_curve_address,
        decoded_data :decoded_input_data :params :delta :: INT AS delta,
        decoded_data :decoded_input_data :params :initialNFTIDs AS initial_nft_ids_array,
        decoded_data :decoded_input_data :params :initialTokenBalance :: INT AS initial_token_balance,
        decoded_data :decoded_input_data :params :spotPrice :: INT AS spot_price,
        decoded_data :decoded_input_data :params :fee :: INT AS fee,
        decoded_data :decoded_input_data :params :nft :: STRING AS nft_address,
        decoded_data :decoded_input_data :params :poolType :: INT AS pool_type,
        decoded_data :decoded_output_data :pair :: STRING AS pool_address,
        decoded_data :decoded_input_data :params :token :: STRING AS token_address,
        _inserted_timestamp
    FROM
        raw_traces
    WHERE
        function_name = 'createPairERC20'
)
SELECT
    *
FROM
    eth_pool
UNION ALL
SELECT
    *
FROM
    erc20_pool