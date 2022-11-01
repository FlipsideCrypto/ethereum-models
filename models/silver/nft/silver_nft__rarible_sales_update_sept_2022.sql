{{ config(
    materialized = 'incremental',
    unique_key = 'nft_uni_id',
    cluster_by = ['block_timestamp::DATE']
) }}

with new_rarible_tx as (
    select 
    block_timestamp, 
    _inserted_timestamp,
    tx_hash 
    from {{ ref('silver__logs') }}
    where block_timestamp >= '2022-09-05'
    and contract_address = '0x9757f2d2b135150bbeb65308d4a91804107cd8d6'
    and origin_to_address = '0x9757f2d2b135150bbeb65308d4a91804107cd8d6'
    and topics[0] = '0x956cd63ee4cdcd81fda5f0ec7c6c36dceda99e1b412f4a650a5d26055dc3c450'

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

base_sales as (            
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
from {{ ref('silver__logs') }}

    WHERE block_timestamp >= '2022-09-05'
    and tx_hash in (select tx_hash from new_rarible_tx)
   
    AND tx_status = 'SUCCESS'
    and event_name in ('Transfer', 'TransferSingle')
    and origin_function_signature in (
        '0xe99a3f80'
        ,'0x0d5f7d35'
    )
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
   
eth_sales as (
    
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
    coalesce (case when to_address = '0x1cf0df2a5a20cd61d68d4489eebbf85b8d39e18a'
              then eth_value end , 0) as platform_fee_raw,
    case when to_address = seller_address then eth_value end as price_raw,
    coalesce (case when to_address != seller_address 
            and to_address != '0x1cf0df2a5a20cd61d68d4489eebbf85b8d39e18a' 
            then eth_value end , 0) as creator_fee_raw 
    from {{ ref('silver__traces') }} t 
    inner join base_sales b on t.tx_hash = b.tx_hash 
    
    where t.block_timestamp >= '2022-09-05'
    and t.eth_value > 0
    and identifier != 'CALL_ORIGIN'

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
    
token_sales as (
    
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
    coalesce (case when event_inputs:to = '0x1cf0df2a5a20cd61d68d4489eebbf85b8d39e18a' 
              then event_inputs:value end, 0) as platform_fee_raw,
    case when event_inputs:to = seller_address then event_inputs:value end as price_raw,
    coalesce (case when event_inputs:to != seller_address 
            and event_inputs:to != '0x1cf0df2a5a20cd61d68d4489eebbf85b8d39e18a' 
            then event_inputs:value end , 0) as creator_fee_raw
    from {{ ref('silver__logs') }} t 
    inner join base_sales b on t.tx_hash = b.tx_hash 
    
    where t.block_timestamp >= '2022-09-05'
    and event_inputs:value is not null 
    and event_name = 'Transfer'

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
    
    agg_sales as (
    select* from eth_sales 
    union all 
    select* from token_sales 
    ),

    all_prices AS (
    SELECT
        HOUR,
        decimals,
        case 
            when symbol is null and token_address is null then 'ETH'
            else symbol end as symbol,
        CASE
            WHEN LOWER(token_address) IS NULL THEN 'ETH'
            ELSE LOWER(token_address)
        END AS currency_address,
        AVG(price) AS price
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        (
            currency_address IN (
                SELECT 
                DISTINCT currency_address 
                FROM eth_sales 
                
                union 
                
                select 
                distinct currency_address 
                from token_sales 
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
                base_sales
        )
    GROUP BY
        HOUR, decimals, 
        symbol,
        token_address
),

eth_price as (
    select 
    hour,
    avg(price) as eth_price_hourly
    from 
        {{ ref('core__fact_hourly_token_prices') }}
    where symbol is null and token_address is null
    group by hour
),

agg_sales_prices as (
select 
    t.block_number,
    t.block_timestamp,
    s.tx_hash, 
    s.event_type,
    s.origin_to_address as platform_address,
    'rarible' as platform_name,
    'rarible' AS platform_exchange_version,
    
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
        when s.currency_address != 'ETH' and p.currency_address is not null 
                then s.price / pow(10, decimals) 
        when p.currency_address is null then s.price
            end as prices,
    
    prices * p.price as price_usd,
    
    case when s.currency_address = 'ETH' then total_fees
        when s.currency_address != 'ETH' and p.currency_address is not null
            then total_fees / pow(10, decimals) 
        when p.currency_address is null then total_fees 
            end as total_fees_adj,
    
    case when s.currency_address = 'ETH' then platform_fee
        when s.currency_address != 'ETH' and p.currency_address is not null
            then platform_fee / pow(10, decimals) 
        when p.currency_address is null then platform_fee 
            end as platform_fee_adj,
    
    case when s.currency_address = 'ETH' then creator_fee
        when s.currency_address != 'ETH' and p.currency_address is not null
            then creator_fee / pow(10, decimals) 
        when p.currency_address is null then creator_fee 
            end as creator_fee_adj,
    
    total_fees_adj * p.price as total_fees_usd,
    platform_fee_adj * p.price as platform_fee_usd,
    creator_fee_adj * p.price as creator_fee_usd,
    prices + total_fees_adj as total_transaction_price,
    price_usd + total_fees_usd as total_transaction_price_usd,

    t.tx_fee,
    t.tx_fee * e.eth_price_hourly as tx_fee_usd ,
    
    
    
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
    
    from agg_sales s 
        inner join {{ ref('silver__transactions') }}  t on t.tx_hash = s.tx_hash 
        left join all_prices p on date_trunc('hour', t.block_timestamp) = p.hour 
                and s.currency_address = p.currency_address
        left join {{ ref('silver__nft_transfers') }} n on n.tx_hash = s.tx_hash and n.contract_address = s.nft_address 
        left join eth_price e on date_trunc('hour', t.block_timestamp) = e.hour 
    
    where t.block_number is not null 
    
    qualify(ROW_NUMBER() over(PARTITION BY nft_uni_id
                                    ORDER BY price_usd DESC)
           ) = 1
)

select
    block_number,
    block_timestamp,
    tx_hash, 
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    seller_address,
    buyer_address,
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
    origin_from_address, 
    origin_to_address,
    origin_function_signature, 
    nft_uni_id,
    _inserted_timestamp,
    input_data

from agg_sales_prices



   