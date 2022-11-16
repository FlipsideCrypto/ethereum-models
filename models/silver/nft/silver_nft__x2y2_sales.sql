{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}


WITH x2y2_txs AS (

    SELECT
        tx_hash,
        _inserted_timestamp,
        CONCAT('0x', SUBSTR(DATA, 1115, 40)) AS nft_address,
        CONCAT('0x', SUBSTR(DATA, 27, 40)) AS to_address,
        udf_hex_to_int(SUBSTR(DATA, 1186, 33)) AS tokenid,
        ROW_NUMBER() over(
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS agg_id,
        CONCAT('0x', SUBSTR(DATA, 91, 40)) AS to_address_token
    FROM
        {{ ref('silver__logs') }}
    WHERE
        tx_status = 'SUCCESS'
        AND contract_address = '0x74312363e45dcaba76c59ec49a7aa8a65a67eed3'
        AND topics [0] = '0x3cbb63f144840e5b1b0a38a7c19211d2e89de4d7c5faf8b2d3c1776c302d1d33'
        
        and block_timestamp >= '2022-01-01'

    {% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
    
last_nft_transfer AS (
    SELECT
        nft.tx_hash,
        nft.contract_address,
        nft.tokenid,
        nft.to_address,
        nft.project_name,
        nft.from_address,
        nft.event_index,
        nft.token_metadata,
        nft.erc1155_value,
        nft._inserted_timestamp,
        nft._log_id
    FROM
        {{ ref('silver__nft_transfers') }}
        nft
        INNER JOIN x2y2_txs
        ON x2y2_txs.tx_hash = nft.tx_hash
        AND x2y2_txs.nft_address = nft.contract_address
        AND x2y2_txs.tokenid = nft.tokenid 

   qualify(ROW_NUMBER() over(PARTITION BY nft.tx_hash, nft.contract_address, nft.tokenid
    ORDER BY
        event_index DESC)) = 1
),

first_nft_transfer AS (
    SELECT
        nft.tx_hash,
        nft.contract_address,
        nft.tokenid,
        nft.to_address,
        nft.from_address AS nft_seller,
        nft.event_index,
        nft.token_metadata,
        nft.erc1155_value,
        nft._inserted_timestamp,
        nft._log_id
    FROM
        {{ ref('silver__nft_transfers') }}
        nft
        INNER JOIN x2y2_txs
        ON x2y2_txs.tx_hash = nft.tx_hash
        AND x2y2_txs.nft_address = nft.contract_address
        AND x2y2_txs.tokenid = nft.tokenid 
        
    qualify(ROW_NUMBER() over(PARTITION BY nft.tx_hash, nft.contract_address, nft.tokenid
    ORDER BY
        event_index ASC)) = 1
),
relevant_transfers AS (
    SELECT
        A.tx_hash,
        A.contract_address,
        A.project_name,
        A.tokenid,
        A.to_address AS buyer_address,
        b.nft_seller AS seller_address,
        B.event_index,
        A.token_metadata,
        A.erc1155_value,
        A._inserted_timestamp,
        A._log_id
    FROM
        last_nft_transfer A
        JOIN first_nft_transfer b
        ON A.tx_hash = b.tx_hash
        AND A.contract_address = b.contract_address
        AND A.tokenid = b.tokenid
),
nft_base AS (
    SELECT
        tx_hash,
        contract_address,
        project_name,
        tokenid,
        buyer_address,
        seller_address,
        event_index,
        token_metadata,
        erc1155_value,
        _inserted_timestamp,
        _log_id,
        ROW_NUMBER() over(
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS agg_id
    FROM
        relevant_transfers

) ,

traces_base_data AS (
    SELECT
        tx_hash,
        from_address,
        to_address,
        eth_value AS amount,
        CASE
            WHEN to_address in ( '0xd823c605807cc5e6bd6fc0d7e4eea50d3e2d66cd', '0x099ba539cde20ff8b87b18460024a9e3acc9e025')
                    THEN 'fee' --fee managment contract
            ELSE 'other'
        END AS payment_type,
        SPLIT(
            identifier,
            '_'
        ) AS split_id,
        split_id [1] :: INTEGER AS level1,
        split_id [2] :: INTEGER AS level2,
        split_id [3] :: INTEGER AS level3,
        split_id [4] :: INTEGER AS level4,
        split_id [5] :: INTEGER AS level5,
        split_id [6] :: INTEGER AS level6,
        split_id [7] :: INTEGER AS level7,
        split_id [8] :: INTEGER AS level8,
        'ETH' AS currency_symbol,
        'ETH' AS currency_address
    FROM
        {{ ref('silver__traces') }}
    WHERE
        eth_value > 0
        AND TYPE = 'CALL'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                x2y2_txs
        )
        AND from_address = '0x74312363e45dcaba76c59ec49a7aa8a65a67eed3' --exchange contract

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
) ,

id_sales_traces AS (
    SELECT
        A.*,
        ROW_NUMBER() over(
            PARTITION BY A.tx_hash
            ORDER BY
                level1 ASC,
                level2 ASC,
                level3 ASC,
                level4 ASC,
                level5 ASC,
                level6 ASC,
                level7 ASC,
                level8 ASC
        ) AS sale_id
    FROM
        traces_base_data A
        INNER JOIN (
            SELECT
                DISTINCT tx_hash,
                seller_address
            FROM
                nft_base
        ) b
        ON b.tx_hash = A.tx_hash
        AND A.to_address = b.seller_address
    WHERE
        payment_type = 'other'

)  ,
traces_group_id AS (
    SELECT
        A.*,
        b.sale_id,
        CASE
            WHEN A.level8 IS NOT NULL THEN CONCAT(
                A.level7,
                '-',
                A.level6,
                '-',
                A.level5,
                '-',
                A.level4,
                '-',
                A.level3,
                '-',
                A.level2,
                '-',
                A.level1
            ) :: STRING
            WHEN A.level7 IS NOT NULL THEN CONCAT(
                A.level6,
                '-',
                A.level5,
                '-',
                A.level4,
                '-',
                A.level3,
                '-',
                A.level2,
                '-',
                A.level1
            ) :: STRING
            WHEN A.level6 IS NOT NULL THEN CONCAT(
                A.level5,
                '-',
                A.level4,
                '-',
                A.level3,
                '-',
                A.level2,
                '-',
                A.level1
            ) :: STRING
            WHEN A.level5 IS NOT NULL THEN CONCAT(
                A.level4,
                '-',
                A.level3,
                '-',
                A.level2,
                '-',
                A.level1
            ) :: STRING
            WHEN A.level4 IS NOT NULL THEN CONCAT(
                A.level3,
                '-',
                A.level2,
                '-',
                A.level1
            ) :: STRING
            WHEN A.level3 IS NOT NULL THEN CONCAT(
                A.level2,
                '-',
                A.level1
            ) :: STRING
            WHEN A.level2 IS NOT NULL THEN A.level1 :: STRING
        END AS group_id
    FROM
        traces_base_data AS A
        LEFT JOIN id_sales_traces b
        ON A.tx_hash = b.tx_hash
        AND A.split_id = b.split_id
    
     
) ,
traces_agg_id AS (
    SELECT
        *,
        LAST_VALUE(sale_id) over(
            PARTITION BY tx_hash,
            group_id
            ORDER BY
                level1 ASC,
                level2 ASC,
                level3 ASC,
                level4 ASC,
                level5 ASC,
                level6 ASC,
                level7 ASC,
                level8 ASC,
                amount ASC
        ) AS agg_id
    FROM
        traces_group_id
   
),
traces_payment_data AS (
    SELECT
        A.tx_hash,
        A.from_address,
        A.to_address,
        A.amount,
        A.currency_symbol,
        A.currency_address,
        A.agg_id,
        b.seller_address AS nft_seller,
        b.buyer_address AS nft_buyer,
        CASE
            WHEN payment_type = 'fee' THEN 'platform_fee'
            WHEN payment_type = 'other'
            AND nft_seller <> A.to_address
            AND nft_buyer <> A.to_address THEN 'creator_fee'
            WHEN payment_type = 'other'
            AND (
                nft_seller = A.to_address
                OR nft_buyer = A.to_address
            ) THEN 'to_seller'
        END AS payment_type
    FROM
        traces_agg_id A
        LEFT JOIN nft_base b
        ON b.tx_hash = A.tx_hash
        AND A.agg_id = b.agg_id

),


token_transfer_data_data AS (
    SELECT
        *,
        CASE
            WHEN to_address in ('0xd823c605807cc5e6bd6fc0d7e4eea50d3e2d66cd', '0x099ba539cde20ff8b87b18460024a9e3acc9e025')
                    THEN 'fee'
            ELSE 'other'
        END AS payment_type,
        SPLIT_PART(
            _log_id,
            '-',
            2
        ) AS event_index
    FROM
        {{ ref('core__ez_token_transfers') }}
    WHERE
        tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                x2y2_txs
        )
        AND from_address = '0x74312363e45dcaba76c59ec49a7aa8a65a67eed3'
),
token_transfer_agg AS (
    SELECT
        tx_hash,
        contract_address AS currency_address,
        from_address,
        to_address,
        symbol AS currency_symbol,
        amount,
        payment_type,
        event_index,
        ROW_NUMBER() over(
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS agg_id,
        CEIL(
            agg_id / 2
        ) AS join_id,
        CEIL(
            agg_id / 3
        ) AS join_id2
    FROM
        token_transfer_data_data
),
token_payment_type AS (
    SELECT
        tx_hash,
        SUM(
            CASE
                WHEN payment_type = 'fee' THEN 1
            END
        ) AS fees_paid,
        SUM(
            CASE
                WHEN payment_type = 'other' THEN 1
            END
        ) AS other_payments,
        CASE
            WHEN fees_paid = other_payments THEN 'join_id1'
            WHEN (
                fees_paid * 2
            ) = other_payments THEN 'join_id2'
            WHEN fees_paid IS NULL THEN 'agg_id'
            WHEN other_payments * 2 = fees_paid THEN 'join_id2'
        END AS join_type
    FROM
        token_transfer_agg
    GROUP BY
        tx_hash
),
token_join_type AS (
    SELECT
        A.*,
        b.join_type,
        CASE
            WHEN join_type = 'join_id1' THEN join_id
            WHEN join_type = 'join_id2' THEN join_id2
            WHEN join_type = 'agg_id' THEN agg_id
        END AS final_join_id
    FROM
        token_transfer_agg AS A
        LEFT JOIN token_payment_type AS b
        ON A.tx_hash = b.tx_hash
),
token_payment_data AS (
    SELECT
        A.tx_hash,
        A.from_address,
        A.to_address,
        A.amount,
        A.currency_symbol,
        A.currency_address,
        A.final_join_id,
        b.to_address AS nft_seller,
        b.to_address_token AS to_address_token,
        CASE
            WHEN A.currency_address = 'ETH' THEN nft_seller
            ELSE to_address_token
        END AS seller_address,
        CASE
            WHEN payment_type = 'fee' THEN 'platform_fee'
            WHEN payment_type = 'other'
            AND seller_address = A.to_address THEN 'to_seller'
            WHEN payment_type = 'other'
            AND seller_address <> A.to_address THEN 'creator_fee'
        END AS payment_type
    FROM
        token_join_type A
        LEFT JOIN x2y2_txs b
        ON b.tx_hash = A.tx_hash
        AND A.final_join_id = b.agg_id
),
all_paymemts AS (
    SELECT
        tx_hash,
        from_address,
        nft_seller,
        amount,
        currency_address,
        currency_symbol,
        payment_type,
        final_join_id
    FROM
        token_payment_data
    UNION ALL
    SELECT
        tx_hash,
        from_address,
        nft_seller,
        amount,
        currency_address,
        currency_symbol,
        payment_type,
        agg_id AS final_join_id
    FROM
        traces_payment_data
),
sale_amount AS (
    SELECT
        tx_hash,
        final_join_id,
        currency_address,
        currency_symbol,
        SUM(amount) AS sale_amount
    FROM
        all_paymemts
    GROUP BY
        tx_hash,
        final_join_id,
        currency_address,
        currency_symbol
),
platform_fees AS (
    SELECT
        tx_hash,
        final_join_id,
        currency_address,
        currency_symbol,
        SUM(amount) AS platform_fee
    FROM
        all_paymemts
    WHERE
        payment_type = 'platform_fee'
    GROUP BY
        tx_hash,
        final_join_id,
        currency_address,
        currency_symbol
),
creator_fees AS (
    SELECT
        tx_hash,
        final_join_id,
        currency_address,
        currency_symbol,
        SUM(amount) AS creator_fee
    FROM
        all_paymemts
    WHERE
        payment_type = 'creator_fee'
    GROUP BY
        tx_hash,
        final_join_id,
        currency_address,
        currency_symbol
),
transaction_data AS (
    SELECT
        tx_hash,
        block_timestamp,
        block_number,
        to_address AS origin_to_address,
        from_address AS origin_from_address,
        tx_fee,
        origin_function_signature,
        input_data,
        _inserted_timestamp
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                x2y2_txs
        )
{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}


),
token_prices AS (
    SELECT
        HOUR,
        case 
            when symbol is null and token_address is null then 'ETH'
            else symbol end as symbol,
        DECIMALS,
        CASE
            WHEN LOWER(token_address) IS NULL THEN 'ETH'
            ELSE LOWER(token_address)
        END AS token_address,
        AVG(price) AS token_price
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        (
            token_address IN (
                SELECT
                    DISTINCT LOWER(currency_address)
                FROM
                    all_paymemts
            )
            OR (
                token_address IS NULL
                AND symbol IS NULL
            )
        )
        AND HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                transaction_data
        )
    GROUP BY
        HOUR,
        token_address,
        symbol,
        decimals
),
eth_prices AS (
    SELECT
        HOUR,
        token_address,
        token_price AS eth_price
    FROM
        token_prices
    WHERE
        token_address = 'ETH'
),
final_nft_data AS (
    SELECT
        A.tx_hash AS tx_hash,
        t.block_number AS block_number,
        t.block_timestamp AS block_timestamp,
        A._log_id AS _log_id,
        A._inserted_timestamp AS _inserted_timestamp,
        A.contract_address AS nft_address,
        'sale' AS event_type,
        '0x74312363e45dcaba76c59ec49a7aa8a65a67eed3' AS platform_address,
        'x2y2' AS platform_name,
        A.project_name AS project_name,
        A.seller_address AS seller_address,
        A.buyer_address AS buyer_address,
        A.tokenid AS tokenId,
        A.erc1155_value AS erc1155_value,
        A.token_metadata AS token_metadata,
        b.currency_address AS currency_address,
        b.currency_symbol AS currency_symbol,
        b.sale_amount AS price,
        COALESCE(
            d.platform_fee,
            0
        ) AS platform_fee,
        COALESCE(
            C.creator_fee,
            0
        ) AS creator_fee,
        COALESCE(
            d.platform_fee,
            0
        ) + COALESCE(
            C.creator_fee,
            0
        ) AS total_fees,
        t.origin_to_address AS origin_to_address,
        t.origin_from_address AS origin_from_address,
        t.origin_function_signature AS origin_function_signature,
        t.tx_fee AS tx_fee,
        ROUND(
            tx_fee * eth_price,
            2
        ) AS tx_fee_usd,
        ROUND(
            price * prices.token_price,
            2
        ) AS price_usd,
        ROUND(
            total_fees * prices.token_price,
            2
        ) AS total_fees_usd,
        ROUND(
            COALESCE(
                d.platform_fee,
                0
            ) * prices.token_price,
            2
        ) AS platform_fee_usd,
        ROUND(
            COALESCE(
                C.creator_fee,
                0
            ) * prices.token_price,
            2
        ) AS creator_fee_usd,
        input_data
    FROM
        nft_base A
        LEFT JOIN sale_amount b
        ON A.tx_hash = b.tx_hash
        AND A.agg_id = b.final_join_id
        LEFT JOIN creator_fees C
        ON A.tx_hash = C.tx_hash
        AND A.agg_id = C.final_join_id
        LEFT JOIN platform_fees d
        ON A.tx_hash = d.tx_hash
        AND A.agg_id = d.final_join_id
        LEFT JOIN transaction_data t
        ON A.tx_hash = t.tx_hash
        LEFT JOIN token_prices prices
        ON prices.hour = DATE_TRUNC(
            'HOUR',
            t.block_timestamp
        )
        AND b.currency_address = prices.token_address
        LEFT JOIN eth_prices
        ON eth_prices.hour = DATE_TRUNC(
            'HOUR',
            t.block_timestamp
        )
),

raw_model as (
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_to_address,
    origin_from_address,
    origin_function_signature,
    event_type,
    platform_address,
    platform_name,
    'x2y2' AS platform_exchange_version,
    buyer_address,
    seller_address,
    nft_address,
    project_name,
    erc1155_value,
    tokenId,
    token_metadata,
    currency_symbol,
    currency_address,
    price,
    price_usd,
    total_fees,
    platform_fee,
    creator_fee,
    total_fees_usd,
    platform_fee_usd,
    creator_fee_usd,
    tx_fee,
    tx_fee_usd,
    _log_id,
    _inserted_timestamp,
    input_data
FROM
    final_nft_data
WHERE
    price IS NOT NULL
    AND buyer_address IS NOT NULL 

    qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
    ),

faulty_single_tx as (
    select 
    tx_hash,
    count(1) as tokenid_count
    from {{ ref('silver__logs') }}
    where tx_hash in (
        select tx_hash 
        from raw_model
        where price = total_fees
    )
    and event_inputs:tokenId is not null 
    and event_name = 'Transfer'
    group by tx_hash
    having tokenid_count = 1 

),

fs_base_sales as (            
select 
    block_timestamp,
    _inserted_timestamp,
    tx_hash, 
    origin_function_signature, 
    origin_from_address, 
    origin_to_address,
    contract_address as nft_address,
    coalesce (event_inputs:_from ::string , event_inputs:from ::string) as seller_address,
    coalesce (event_inputs:_to ::string , event_inputs:to :: string) as buyer_address,
    coalesce (event_inputs:_id ::string , event_inputs:tokenId ::string) as tokenId,
    event_inputs:_value::string as erc1155_value, 
    case when origin_from_address = seller_address then 'bid_won'
        when origin_from_address = buyer_address then 'sale'
        else 'sale' end as event_type 
from {{ref('silver__logs')}}

    WHERE block_timestamp >= '2022-02-01'
    and tx_hash in (select tx_hash from faulty_single_tx)
   
    AND tx_status = 'SUCCESS'
    and event_name in ('Transfer', 'TransferSingle')
    and (event_inputs:_id is not null or event_inputs:tokenId is not null)
    and seller_address != '0x0000000000000000000000000000000000000000'
{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
   ),
   
fs_eth_sales as (
    
    select 
    tx_hash, 
    origin_function_signature, 
    origin_from_address, 
    origin_to_address,
    nft_address, 
    seller_address,
    buyer_address,
    tokenId,
    erc1155_value,
    event_type,
    'ETH' as currency_address,
    sum(price_raw) as price,
    sum(platform_fee_raw) as platform_fee,
    sum(creator_fee_raw) as creator_fee,
    platform_fee + creator_fee as total_fees
    
    from ( 
    select 
    b.block_timestamp,
    t._inserted_timestamp,
    t.tx_hash, 
    b.origin_function_signature, 
    b.origin_from_address, 
    b.origin_to_address,
    b.nft_address,
    seller_address,
    buyer_address,
    tokenId,
    erc1155_value, 
    event_type,
    coalesce (case when to_address in ('0xd823c605807cc5e6bd6fc0d7e4eea50d3e2d66cd', '0x099ba539cde20ff8b87b18460024a9e3acc9e025')
              then eth_value end , 0) as platform_fee_raw,
    coalesce (case when to_address = seller_address then eth_value end ,0) as price_raw,
    coalesce (case when to_address != seller_address 
            and to_address not in ('0xd823c605807cc5e6bd6fc0d7e4eea50d3e2d66cd', '0x099ba539cde20ff8b87b18460024a9e3acc9e025')
            then eth_value end , 0) as creator_fee_raw 
    from {{ref('silver__traces')}} t 
    inner join fs_base_sales b on t.tx_hash = b.tx_hash 
    
    where t.block_timestamp >= '2022-02-01'
    and t.eth_value > 0
    and identifier != 'CALL_ORIGIN'
        and from_address = '0x74312363e45dcaba76c59ec49a7aa8a65a67eed3'


         )
    
    group by tx_hash, 
    origin_function_signature, 
    origin_from_address, 
    origin_to_address,
    nft_address, 
    seller_address,
    buyer_address,
    tokenId,
    erc1155_value, event_type, currency_address
    
    ),
    
fs_token_sales as (
    
    select 
    tx_hash, 
    origin_function_signature, 
    origin_from_address, 
    origin_to_address,
    nft_address, 
    seller_address,
    buyer_address,
    tokenId,
    erc1155_value,
    event_type,
    currency_address,
    sum(price_raw) as price,
    sum(platform_fee_raw) as platform_fee,
    sum(creator_fee_raw) as creator_fee,
    platform_fee + creator_fee as total_fees
    
    from ( 
    select 
    b.block_timestamp,
    t._inserted_timestamp,
    t.tx_hash, 
    b.origin_function_signature, 
    b.origin_from_address, 
    b.origin_to_address,
    b.nft_address,
    seller_address,
    buyer_address,
    tokenId,
    erc1155_value, 
    event_type,
    lower(t.contract_address) as currency_address,
    coalesce (case when event_inputs:to in ('0xd823c605807cc5e6bd6fc0d7e4eea50d3e2d66cd', '0x099ba539cde20ff8b87b18460024a9e3acc9e025')
              then event_inputs:value end, 0) as platform_fee_raw,
    coalesce (case when event_inputs:to = seller_address 
                then event_inputs:value end, 0) as price_raw,
    coalesce (case when event_inputs:to != seller_address 
            and event_inputs:to not in ('0xd823c605807cc5e6bd6fc0d7e4eea50d3e2d66cd', '0x099ba539cde20ff8b87b18460024a9e3acc9e025')
            then event_inputs:value end , 0) as creator_fee_raw
    from {{ref('silver__logs')}} t 
    inner join fs_base_sales b on t.tx_hash = b.tx_hash 
    
    where t.block_timestamp >= '2022-02-01'
    and event_inputs:value is not null 
    and event_name = 'Transfer'
      and event_inputs:from = '0x74312363e45dcaba76c59ec49a7aa8a65a67eed3'
    
        )
    
    group by tx_hash, 
    origin_function_signature, 
    origin_from_address, 
    origin_to_address,
    nft_address, 
    seller_address,
    buyer_address,
    tokenId,
    erc1155_value, event_type, currency_address
    
    ),
    
    fs_agg_sales as (
    select* from fs_eth_sales 
    union all 
    select* from fs_token_sales 
    ),
    
    fs_agg_sales_prices as (
select 
    t.block_number,
    t.block_timestamp,
    s.tx_hash, 
    s.event_type,
    s.origin_to_address as platform_address,
    'x2y2' as platform_name,
    'x2y2' AS platform_exchange_version,
    
    seller_address,
    buyer_address,
    s.nft_address, 
    n.project_name, 
    
    s.erc1155_value,
    s.tokenId,
    n.token_metadata,
    
    p.symbol as currency_symbol,
    s.currency_address, 
    
    case when s.currency_address = 'ETH' then s.price
        when s.currency_address != 'ETH' and p.token_address is not null 
                then s.price / pow(10, decimals) 
        when p.token_address is null then s.price
            end as prices,
    
    prices * p.token_price as price_usd,
    
    case when s.currency_address = 'ETH' then total_fees
        when s.currency_address != 'ETH' and p.token_address is not null
            then total_fees / pow(10, decimals) 
        when p.token_address is null then total_fees 
            end as total_fees_adj,
    
    case when s.currency_address = 'ETH' then platform_fee
        when s.currency_address != 'ETH' and p.token_address is not null
            then platform_fee / pow(10, decimals) 
        when p.token_address is null then platform_fee 
            end as platform_fee_adj,
    
    case when s.currency_address = 'ETH' then creator_fee
        when s.currency_address != 'ETH' and p.token_address is not null
            then creator_fee / pow(10, decimals) 
        when p.token_address is null then creator_fee 
            end as creator_fee_adj,
    
    total_fees_adj * p.token_price as total_fees_usd,
    platform_fee_adj * p.token_price as platform_fee_usd,
    creator_fee_adj * p.token_price as creator_fee_usd,
    prices + total_fees_adj as total_transaction_price,
    price_usd + total_fees_usd as total_transaction_price_usd,

    t.tx_fee,
    t.tx_fee * e.eth_price as tx_fee_usd ,
    
    
    
    s.origin_from_address, 
    s.origin_to_address,
    s.origin_function_signature, 
    concat(
            s.tx_hash,
            '-',
            s.tokenId,
            '-',
            COALESCE(
                s.erc1155_value,
                0
            )
        ) AS nft_uni_id,
    t._inserted_timestamp,
    t.input_data
    
    from fs_agg_sales s 
        inner join {{ref('silver__transactions')}}  t
             on t.tx_hash = s.tx_hash 
        left join token_prices p 
            on date_trunc('hour', t.block_timestamp) = p.hour 
                and s.currency_address = p.token_address
        left join {{ref('silver__nft_transfers')}} n 
            on n.tx_hash = s.tx_hash and n.contract_address = s.nft_address 
        left join eth_prices e 
            on date_trunc('hour', t.block_timestamp) = e.hour 
    
    where t.block_number is not null 
    
    qualify(ROW_NUMBER() over(PARTITION BY nft_uni_id
                                    ORDER BY price_usd DESC)
           ) = 1
),

faulty_single_tx_final as (
select
    block_number,
    block_timestamp,
    tx_hash, 
    origin_from_address, 
    origin_to_address,
    origin_function_signature,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    buyer_address,
    seller_address,
    nft_address, 
    project_name, 
    erc1155_value,
    tokenId,
    token_metadata,
    currency_symbol,
    currency_address, 
    total_transaction_price as price,
    total_transaction_price_usd as price_usd,
    total_fees_adj as total_fees,
    platform_fee_adj as platform_fee,
    creator_fee_adj as creator_fee,
    total_fees_usd,
    platform_fee_usd,
    creator_fee_usd,
    tx_fee,
    tx_fee_usd ,    
    nft_uni_id,
    _inserted_timestamp,
    input_data

from fs_agg_sales_prices
    ),
    
regular_tx as 
(
select 
    * 
    from raw_model

where tx_hash not in (
    select tx_hash
        from faulty_single_tx
    )
),

final as (
    
    select * from regular_tx
    union all 
    select * from faulty_single_tx_final
    )


SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_to_address,
    origin_from_address,
    origin_function_signature,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    buyer_address,
    seller_address,
    nft_address,
    project_name,
    erc1155_value,
    tokenId,
    token_metadata,
    currency_symbol,
    currency_address,
    price,
    price_usd,
    total_fees,
    platform_fee,
    creator_fee,
    total_fees_usd,
    platform_fee_usd,
    creator_fee_usd,
    tx_fee,
    tx_fee_usd,
    _log_id,
    _inserted_timestamp,
    input_data
FROM
    final

    qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
