{{ config(
    materialized = 'incremental',
    unique_key = '_log_id_nft',
    cluster_by = ['block_timestamp::DATE']
) }}

with seaport_tx_table AS (
    SELECT
        tx_hash,
        event_index,
        contract_address,
        topics,
        data,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE block_timestamp >= '2022-01-01'
    and contract_address = '0x00000000006c3852cbef3e08e8df289169ede581'

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(
            ingested_at
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),

 base AS (
    SELECT  
        l.tx_hash,
        l.event_index,
        l.contract_address,
        l.topics,
        l.data,
        A.data AS abi,
        _log_id,
        _inserted_timestamp
    FROM
        seaport_tx_table l
        JOIN {{ ref('silver__abis') }} A USING (contract_address)
) ,
decoded AS (
    SELECT
        tx_hash,
        event_index,
        contract_address,
        abi,
        ethereum.streamline.udf_decode(
            abi,
            OBJECT_CONSTRUCT(
                'topics',
                topics,
                'data',
                DATA,
                'address',
                contract_address
            )
        ) AS decoded_output,
        decoded_output [0] :name :: STRING AS event_name,
        array_size(decoded_output[0]:data[4]:value) as offer_length,
    
        case when decoded_output[0]:data[4]:value[0][0] in (2,3) then 'buy'
            when decoded_output[0]:data[4]:value[0][0] in (0,1) then 'offer_accepted'
        end as trade_type ,
        _log_id,
        _inserted_timestamp
    FROM
        base
),

flat AS (
    SELECT
        tx_hash,
        event_index,
        contract_address,
        event_name,
        offer_length,
        trade_type, 
        decoded_output[0]:data as full_data,
        _log_id,
        _inserted_timestamp,
        OBJECT_AGG(
            VALUE :name,
            VALUE :value
        ) AS decoded_output
    FROM
        decoded,
        LATERAL FLATTEN(
            input => decoded_output [0] :data
        ) f
    WHERE
        event_name = 'OrderFulfilled'
    and trade_type is not null
    and decoded_output[0]:data[5]:value[0] is not null
    GROUP BY
        tx_hash,
        event_index,
        contract_address,
        event_name,
        offer_length,
        trade_type, 
        full_data,
        _log_id,
        _inserted_timestamp
    
  )
  ,
  
  filtered_private_offer_tx as (
  
  select 
        tx_hash 

    from flat       
    where 
        trade_type = 'offer_accepted' 
    and 
        full_data[3]:value::string = '0x0000000000000000000000000000000000000000'
      ),
  
  private_offer_tx_flat as (
  select 
      tx_hash, 
      array_agg (
      case 
        when trade_type = 'buy' then full_data[1]:value::string end)[0]::string as private_offerer ,
      array_agg(
      case 
        when trade_type = 'offer_accepted' then full_data[1]:value::string end)[0]::string as private_recipient
      
      from flat 
  where tx_hash in
      (select 
       tx_hash 
      from filtered_private_offer_tx
      )
      group by tx_hash 
  ),

base_sales_buy AS (

    SELECT
        tx_hash,
        event_index,
        contract_address,
        event_name,
        offer_length,
        decoded_output :offerer :: STRING AS offerer, --seller 
        decoded_output :orderHash :: STRING AS orderHash,
        decoded_output :recipient :: STRING AS recipient_temp, -- buyer 
        case 
            when recipient_temp = '0x0000000000000000000000000000000000000000' then 'private'
            else 'public'
            end as sale_category,
        trade_type,
        iff(offer_length > 1, 'true', 'false') as is_price_estimated,
        decoded_output :zone :: STRING AS ZONE,
        decoded_output :consideration [0] [0] AS tx_type,
    
        decoded_output,
        decoded_output :consideration AS consideration,
        decoded_output :offer AS offer,
        _log_id,
        _inserted_timestamp
    FROM
        flat
    WHERE
        event_name = 'OrderFulfilled'
    AND trade_type = 'buy'
    and tx_hash not in (
    select tx_hash from filtered_private_offer_tx
    )
    and tx_type is not null 
    ), 

base_sales_buy_public_nft_transfers as (

select 
    tx_hash, 
    event_index, 
    t.index as flatten_index, -- 2 = erc721, 3 = erc1155
    t.value[0] as token_type,
    t.value[1]::string as nft_address,
    t.value[2]::string as tokenid,
    t.value[3] as number_of_item_or_erc1155 -- if token type = 3 then erc1155_value, else null 
    
    FROM
        flat, table(flatten(input => decoded_output:offer)) t 
    WHERE 
        tx_hash in (
            select 
            tx_hash 
            from
            base_sales_buy
            where sale_category = 'public'
        )
    and t.value[0]::string in (2,3)
    and nft_address is not null
),
    
base_sales_buy_private_nft_transfers as (

select 
    tx_hash, 
    event_index, 
    t.index as flatten_index,
    t.value[0] as token_type,
    t.value[1]::string as nft_address,
    t.value[2]::string as tokenid,
    t.value[3] as number_of_item_or_erc1155,
    t.value[4]::string as private_sale_recipient 
    
    FROM
        flat, table(flatten(input => decoded_output:consideration)) t 
    WHERE 
        tx_hash in (
            select 
            tx_hash 
            from
            base_sales_buy
            where sale_category = 'private'
        )
    and t.value[0]::string in (2,3)
    and nft_address is not null
),



base_sales_buy_sale_amount_filter as (

select 
    tx_hash, 
    event_index, 
    t.value
    
    FROM
        flat, table(flatten(input => decoded_output:consideration)) t 
    WHERE 
        tx_hash in (
            select 
            tx_hash 
            from
            base_sales_buy
        )
    and t.value[0]::string in (0,1)
),

 base_sales_buy_sale_amount as (
    select 
        tx_hash, 
        event_index,
        array_agg(value) as sale_values,
        sale_values[0][1]::string as currency_address,
        coalesce (sale_values[0][3]::int, 0 ) as sale_amount_raw_, 
        coalesce (sale_values[1][3]::int, 0 ) as platform_fee_raw_, 
        coalesce (sale_values[2][3]::int, 0 ) as creator_fee_raw_1_, 
        coalesce (sale_values[3][3]::int, 0 ) as creator_fee_raw_2_, 
        coalesce (sale_values[4][3]::int, 0 ) as creator_fee_raw_3_, 
        coalesce (sale_values[5][3]::int, 0 ) as creator_fee_raw_4_
        
    from base_sales_buy_sale_amount_filter
    
    group by tx_hash, event_index
    
    ),
    
    base_sales_buy_final_public as (
    
        select 
        b.tx_hash,
        b.event_index,
        b.contract_address,
        b.event_name,
        b.offer_length,
        b.offerer, --seller 
        b.orderHash,
        recipient_temp as recipient, -- buyer 
        sale_category,
        trade_type,
        is_price_estimated,
        zone,
        tx_type,
         
        n.token_type, -- 2 = erc721, 3 = erc1155
        n.nft_address,
        n.tokenid,
        case
            when n.token_type = '3' then n.number_of_item_or_erc1155
            else null
        end as erc1155_value,
        
        currency_address,
        sale_amount_raw_ / b.offer_length as sale_amount_raw,
        platform_fee_raw_ / b.offer_length as platform_fee_raw,
        creator_fee_raw_1_ / b.offer_length as creator_fee_raw_1,
        creator_fee_raw_2_ / b.offer_length as creator_fee_raw_2,
        creator_fee_raw_3_ / b.offer_length as creator_fee_raw_3, 
        creator_fee_raw_4_ / b.offer_length as creator_fee_raw_4,
        creator_fee_raw_1 + creator_fee_raw_2 + creator_fee_raw_3 + creator_fee_raw_4 as total_creator_fees_raw, 
        creator_fee_raw_1 + creator_fee_raw_2 + creator_fee_raw_3 + creator_fee_raw_4 + platform_fee_raw as total_fees_raw,
        total_fees_raw + sale_amount_raw as total_sale_amount_raw,
        
        decoded_output,
        consideration,
        offer,
        _log_id,
        _inserted_timestamp
    
    from base_sales_buy b 
        inner join base_sales_buy_sale_amount s 
            on b.tx_hash = s.tx_hash
            and b.event_index = s.event_index
        
        full outer join base_sales_buy_public_nft_transfers n 
            on b.tx_hash = n.tx_hash 
            and b.event_index = n.event_index
        
        where nft_address is not null 
        ),
        
    base_sales_buy_final_private as (
    
        select 
        b.tx_hash,
        b.event_index,
        b.contract_address,
        b.event_name,
        b.offer_length,
        b.offerer, --seller 
        b.orderHash,
        private_sale_recipient as recipient,
        sale_category,
        trade_type,
        is_price_estimated,
        zone,
        tx_type,
         
        n.token_type, -- 2 = erc721, 3 = erc1155
        n.nft_address,
        n.tokenid,
        case
            when n.token_type = '3' then n.number_of_item_or_erc1155
            else null
        end as erc1155_value,
        
        currency_address,
        sale_amount_raw_ / b.offer_length as sale_amount_raw,
        platform_fee_raw_ / b.offer_length as platform_fee_raw,
        creator_fee_raw_1_ / b.offer_length as creator_fee_raw_1,
        creator_fee_raw_2_ / b.offer_length as creator_fee_raw_2,
        creator_fee_raw_3_ / b.offer_length as creator_fee_raw_3, 
        creator_fee_raw_4_ / b.offer_length as creator_fee_raw_4,
        creator_fee_raw_1 + creator_fee_raw_2 + creator_fee_raw_3 + creator_fee_raw_4 as total_creator_fees_raw, 
        creator_fee_raw_1 + creator_fee_raw_2 + creator_fee_raw_3 + creator_fee_raw_4 + platform_fee_raw as total_fees_raw,
        total_fees_raw + sale_amount_raw as total_sale_amount_raw,
        
        decoded_output,
        consideration,
        offer,
        _log_id,
        _inserted_timestamp
    
    from base_sales_buy b 
        inner join base_sales_buy_sale_amount s 
            on b.tx_hash = s.tx_hash
            and b.event_index = s.event_index
        
        full outer join base_sales_buy_private_nft_transfers n 
            on b.tx_hash = n.tx_hash 
            and b.event_index = n.event_index
        
        where nft_address is not null
    ),
    
    base_sales_buy_final as (
        
    select * 
        from base_sales_buy_final_public 
        
    union all
        
    select * 
        from base_sales_buy_final_private
    
    ),
    
    base_sales_offer_accepted as (

    SELECT
        tx_hash,
        event_index,
        contract_address,
        event_name,
        offer_length,
        decoded_output :recipient :: STRING AS offerer_temp, --seller 
        decoded_output :orderHash :: STRING AS orderHash,
        decoded_output :offerer :: STRING AS recipient_temp, -- buyer 
        case 
            when offerer_temp = '0x0000000000000000000000000000000000000000' then 'private'
            else 'public'
            end as sale_category,
        trade_type,
        iff(offer_length > 1, 'true', 'false') as is_price_estimated,
        decoded_output :zone :: STRING AS ZONE,
        decoded_output :consideration [0] [0] AS tx_type,
        
        decoded_output:offer[0][3]::int / offer_length as total_sale_amount_raw,
        
        decoded_output,
        decoded_output :consideration AS consideration,
        decoded_output :offer AS offer,
        _log_id,
        _inserted_timestamp
    FROM
        flat
    WHERE
        event_name = 'OrderFulfilled'
    AND trade_type = 'offer_accepted'
    and tx_type is not null 
    and tx_hash is not null 
    )
    ,
    
    base_sales_offer_accepted_nft_transfers as (

select 
    tx_hash, 
    event_index, 
    t.index as flatten_index,
    t.value[0] as token_type,
    t.value[1]::string as nft_address,
    t.value[2]::string as tokenid,
    t.value[3] as number_of_item_or_erc1155
    
    FROM
        flat, table(flatten(input => decoded_output:consideration)) t 
    WHERE 
        tx_hash in (
            select 
            tx_hash 
            from
            base_sales_offer_accepted
        )
    and t.value[0]::string in (2,3)
),

base_sales_offer_accepted_sale_amount_filter as (

select 
    tx_hash, 
    event_index, 
    t.value
    
    FROM
        flat, table(flatten(input => decoded_output:consideration)) t 
    WHERE 
        tx_hash in (
            select 
            tx_hash 
            from
            base_sales_offer_accepted
        )
    and t.value[0]::string in (0,1)
    and trade_type = 'offer_accepted'
),

 base_sales_offer_accepted_sale_amount as (
    select 
        tx_hash, 
        event_index,
        array_agg(value) as sale_values,
        sale_values[0][1]::string as currency_address,
        coalesce (sale_values[0][3]::int, 0 ) as platform_fee_raw_, 
        coalesce (sale_values[1][3]::int, 0 ) as creator_fee_raw_1_, 
        coalesce (sale_values[2][3]::int, 0 ) as creator_fee_raw_2_, 
        coalesce (sale_values[3][3]::int, 0 ) as creator_fee_raw_3_, 
        coalesce (sale_values[4][3]::int, 0 ) as creator_fee_raw_4_
        
    from base_sales_offer_accepted_sale_amount_filter
    
    group by tx_hash, event_index
    
    ),
    
    base_sales_offer_accepted_final as (
    select 
        b.tx_hash,
        b.event_index,
        contract_address,
        event_name,
        offer_length,
        case when sale_category = 'public' then offerer_temp else private_offerer end as offerer,
        orderHash,
        case when sale_category = 'public' then recipient_temp else private_recipient end as recipient,
        sale_category,
        trade_type,
        is_price_estimated,
        zone,
        tx_type,
        
        token_type,
        nft_address,
        tokenid,
        case when token_type = '3' then number_of_item_or_erc1155 else null end as erc1155_value,
        currency_address,
        decoded_output:offer[0][3]::int / offer_length as total_sale_amount_raw,
        platform_fee_raw_ ::int / offer_length as platform_fee_raw,
        creator_fee_raw_1_ ::int / offer_length as creator_fee_raw_1,
        creator_fee_raw_2_ ::int / offer_length as creator_fee_raw_2,
        creator_fee_raw_3_ ::int / offer_length as creator_fee_raw_3,
        creator_fee_raw_4_ ::int / offer_length as creator_fee_raw_4,
        creator_fee_raw_1 + creator_fee_raw_2 + creator_fee_raw_3 + creator_fee_raw_4 as total_creator_fees_raw,
        platform_fee_raw + total_creator_fees_raw as total_fees_raw,
        
        decoded_output,
        decoded_output :consideration AS consideration,
        decoded_output :offer AS offer,
        _log_id,
        _inserted_timestamp
        
        
        from base_sales_offer_accepted b 
        
        inner join base_sales_offer_accepted_sale_amount s 
            on b.tx_hash = s.tx_hash 
            and b.event_index = s.event_index
        
        left outer join private_offer_tx_flat p 
            on b.tx_hash = p.tx_hash 
        
        full outer join base_sales_offer_accepted_nft_transfers n 
            on b.tx_hash = n.tx_hash 
            and b.event_index = b.event_index

    where nft_address is not null
    
    ),
    
    
    base_sales_buy_and_offer as (
    
        select 
        tx_hash,
        event_index,
        contract_address,
        event_name,
        offer_length,
        offerer, --seller 
        orderHash,
        recipient, -- buyer 
        sale_category,
        trade_type,
        is_price_estimated,
        zone,
        tx_type,
         
        token_type, -- 2 = erc721, 3 = erc1155
        nft_address,
        tokenid as tokenId,
        erc1155_value,
        
        iff(currency_address = '0x0000000000000000000000000000000000000000', 'ETH', currency_address) as currency_address,
        total_sale_amount_raw, 
        total_fees_raw,
        platform_fee_raw , 
        total_creator_fees_raw ,
        creator_fee_raw_1 ,
        creator_fee_raw_2 ,
        creator_fee_raw_3 ,
        creator_fee_raw_4 ,
        
        
        decoded_output,
        consideration,
        offer,
        _log_id,
        _inserted_timestamp
        from base_sales_buy_final
        
        union all 
        
        select 
        tx_hash,
        event_index,
        contract_address,
        event_name,
        offer_length,
        offerer, --seller 
        orderHash,
        recipient, -- buyer 
        sale_category,
        trade_type,
        is_price_estimated,
        zone,
        tx_type,
         
        token_type, -- 2 = erc721, 3 = erc1155
        nft_address,
        tokenid as tokenId,
        erc1155_value,
        
        iff(currency_address = '0x0000000000000000000000000000000000000000', 'ETH', currency_address) as currency_address,
        total_sale_amount_raw, 
        total_fees_raw,
        platform_fee_raw , 
        total_creator_fees_raw ,
        creator_fee_raw_1 ,
        creator_fee_raw_2 ,
        creator_fee_raw_3 ,
        creator_fee_raw_4 ,
        
        
        decoded_output,
        consideration,
        offer,
        _log_id,
        _inserted_timestamp
        from base_sales_offer_accepted_final
    
    ),
    
    all_prices AS (
    SELECT
        HOUR,
        case 
            when symbol is null and token_address is null then 'ETH'
            else symbol end as symbol,
        CASE
            WHEN LOWER(token_address) IS NULL THEN 'ETH'
            ELSE LOWER(token_address)
        END AS currency_address,
        case 
            when currency_address = 'ETH' then '18'
            else decimals
            end as decimals,
        AVG(price) AS hourly_prices 
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        (
            currency_address IN (
                SELECT 
                DISTINCT currency_address 
                FROM base_sales_buy_and_offer 
            )
            OR (
                token_address IS NULL
                AND symbol IS NULL
            )
        )
        AND HOUR::date >= '2022-01-01'
    

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(
            ingested_at
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}

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
    where hour::date >= '2022-01-01'
    and 
        symbol is null 
    and 
        token_address is null
    group by hour
),

tx_data AS (
    SELECT
        tx_hash,
        block_timestamp,
        block_number,
        from_address,
        to_address,
        origin_function_signature,
        tx_fee,
        input_data
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        block_timestamp :: DATE >= '2022-01-01'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                base_sales_buy_and_offer
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

nft_transfers AS (
    SELECT
        tx_hash,
        event_index,
        contract_address,
        project_name,
        tokenId,
        erc1155_value,
        token_metadata,
        CONCAT(
            tx_hash,
            '-',
            contract_address,
            '-',
            tokenId
        ) AS nft_id
    FROM
        {{ ref('silver__nft_transfers') }}
    WHERE
        block_timestamp :: DATE >= '2022-01-01'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                base_sales_buy_and_offer
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

    final_seaport_sales as (
    select 
        t.block_number,
        t.block_timestamp,
        s.tx_hash,
        s.event_index,
        s.contract_address as platform_address,
        'seaport_1_1' as platform_exchange_version,
        s.event_name, -- not needed in final 
        offer_length, -- not needed in final
        offerer as seller_address, --seller 
        orderHash, -- not needed
        recipient as buyer_address, -- buyer 
        sale_category,
        trade_type,
        is_price_estimated,
        zone,
        tx_type,
         
        s.token_type, -- 2 = erc721, 3 = erc1155
        s.nft_address,
        n.project_name,
        s.tokenId,
        s.erc1155_value,
        n.token_metadata,
        p.symbol as currency_symbol,
        s.currency_address,
        total_sale_amount_raw / pow(10,decimals) as price, 
        price * hourly_prices as price_usd,
    
        total_fees_raw / pow(10,decimals) as total_fees,
        total_fees * hourly_prices as total_fees_usd, 
    
        platform_fee_raw / pow(10,decimals) as platform_fee, 
        platform_fee * hourly_prices as platform_fee_usd, 
    
        total_creator_fees_raw / pow(10,decimals) as total_creator_fees,
        total_creator_fees * hourly_prices as total_creator_fees_usd,
    
        creator_fee_raw_1 / pow(10,decimals) as creator_fee_1,
        creator_fee_raw_2 / pow(10,decimals) as creator_fee_2,
        creator_fee_raw_3 / pow(10,decimals) as creator_fee_3,
        creator_fee_raw_4 / pow(10,decimals) as creator_fee_4,
        
        t.tx_fee, 
        t.tx_fee * eth_price_hourly as tx_fee_usd,
        t.from_address as origin_from_address,
        t.to_address as origin_to_address,
        t.origin_function_signature,
        decoded_output,
        consideration,
        offer,
        concat(_log_id, '-', s.nft_address, '-', s.tokenId) as _log_id_nft,
        _inserted_timestamp
    
    from base_sales_buy_and_offer s 

      inner join tx_data t 
            on t.tx_hash = s.tx_hash 

        left join all_prices p 
            on date_trunc('hour', t.block_timestamp) = p.hour 
            and s.currency_address = p.currency_address

        left join nft_transfers n 
            on n.tx_hash = s.tx_hash 
            and n.contract_address = s.nft_address 
            and n.tokenId = s.tokenId

        left join eth_price e 
            on date_trunc('hour', t.block_timestamp) = e.hour 

    )

    SELECT 
        * 
    FROM 
        final_seaport_sales
    
    qualify(ROW_NUMBER() over(PARTITION BY _log_id_nft
    ORDER BY
    _inserted_timestamp DESC)) = 1
    
    
    
    