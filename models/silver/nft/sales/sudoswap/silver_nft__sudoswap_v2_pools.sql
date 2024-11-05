{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH raw_traces AS (

    SELECT
        *,
        decoded_data :function_name :: STRING AS function_name
    FROM
        {{ ref('silver__decoded_traces') }}
    WHERE
        block_timestamp :: DATE >= '2023-05-01'
        AND to_address = '0xa020d57ab0448ef74115c112d18a9c231cc86000'
        AND trace_succeeded
        AND TYPE = 'CALL'
        AND function_name IN (
            'createPairERC1155ETH',
            'createPairERC1155ERC20',
            'createPairERC721ETH',
            'createPairERC721ERC20'
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
erc721_eth_pool AS (
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
        decoded_data :decoded_input_data :_propertyChecker :: STRING property_checker,
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
        function_name = 'createPairERC721ETH'
),
erc721_token_pool AS (
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
        decoded_data :decoded_input_data :params :propertyChecker :: STRING property_checker,
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
        function_name = 'createPairERC721ERC20'
),
erc1155_eth_pool AS (
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
        decoded_data :decoded_input_data :_initialNFTBalance AS initial_nft_balance,
        decoded_data :decoded_input_data :_nftId :: STRING AS nft_id,
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
        function_name = 'createPairERC1155ETH'
),
erc1155_token_pool AS (
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
        decoded_data :decoded_input_data :params :initialNFTBalance :: INT AS initial_nft_balance,
        decoded_data :decoded_input_data :params :nftId :: STRING AS nft_id,
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
        function_name = 'createPairERC1155ERC20'
),
all_pools AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        trace_index,
        function_name,
        asset_recipient,
        bonding_curve_address,
        delta,
        initial_nft_ids_array AS initial_nft_id,
        -- nft_id column name for erc1155s
        NULL AS initial_nft_balance,
        -- only for 1155s
        NULL AS initial_token_balance,
        -- erc20 balance , will be null for eth pool
        token_address,
        property_checker,
        spot_price,
        fee,
        nft_address,
        pool_type,
        pool_address,
        _inserted_timestamp
    FROM
        erc721_eth_pool
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        trace_index,
        function_name,
        asset_recipient,
        bonding_curve_address,
        delta,
        initial_nft_ids_array AS initial_nft_id,
        NULL AS initial_nft_balance,
        initial_token_balance,
        token_address,
        property_checker,
        spot_price,
        fee,
        nft_address,
        pool_type,
        pool_address,
        _inserted_timestamp
    FROM
        erc721_token_pool
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        trace_index,
        function_name,
        asset_recipient,
        bonding_curve_address,
        delta,
        ARRAY_CONSTRUCT(nft_id) AS initial_nft_id,
        initial_nft_balance,
        NULL AS initial_token_balance,
        token_address,
        NULL AS property_checker,
        spot_price,
        fee,
        nft_address,
        pool_type,
        pool_address,
        _inserted_timestamp
    FROM
        erc1155_eth_pool
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        trace_index,
        function_name,
        asset_recipient,
        bonding_curve_address,
        delta,
        ARRAY_CONSTRUCT(nft_id) AS initial_nft_id,
        initial_nft_balance,
        initial_token_balance,
        token_address,
        NULL AS property_checker,
        spot_price,
        fee,
        nft_address,
        pool_type,
        pool_address,
        _inserted_timestamp
    FROM
        erc1155_token_pool
)
SELECT
    *
FROM
    all_pools qualify ROW_NUMBER() over (
        PARTITION BY pool_address
        ORDER BY
            block_timestamp DESC
    ) = 1
