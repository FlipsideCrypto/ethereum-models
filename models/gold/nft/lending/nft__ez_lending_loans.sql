{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' } } }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_type,
    platform_name,
    platform_address,
    platform_exchange_version,
    loanId,
    loanid AS loan_id,
    unique_loan_id,
    lender_address,
    borrower_address,
    project_name,
    project_name AS name,
    nft_address,
    nft_address AS contract_address,
    tokenId,
    tokenid AS token_id,
    loan_token_address,
    loan_token_symbol,
    principal_unadj,
    principal,
    principal_usd,
    debt_unadj,
    debt,
    debt_usd,
    platform_fee_unadj,
    platform_fee,
    platform_fee_usd,
    interest_rate,
    apr,
    loan_term_type,
    loan_start_timestamp,
    loan_due_timestamp,
    loan_tenure,
    tx_fee,
    tx_fee_usd,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    COALESCE (
        nft_lending_id,
        {{ dbt_utils.generate_surrogate_key(
            ['block_number','platform_name','platform_exchange_version']
        ) }}
    ) AS ez_nft_lending_loans_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver_nft__complete_loans') }}
