{{ config(
    materialized='table'
) }} 

WITH contract_deployments AS (
    SELECT 
        tx_hash,
        trace_index,
        from_address as deployer_address,
        to_address as deployed_contract,
        block_timestamp,
        block_number,
        input as bytecode,
        
        -- Error message pattern detection (UniswapV2 signatures)
        CASE WHEN input LIKE '%556e697377617056323a20464f5242494444454e%' THEN 1 ELSE 0 END as has_forbidden_error,
        CASE WHEN input LIKE '%556e697377617056323a204944454e544943414c%' THEN 1 ELSE 0 END as has_identical_error,
        CASE WHEN input LIKE '%556e697377617056323a205a45524f5f41444452455353%' THEN 1 ELSE 0 END as has_zero_address_error,
        CASE WHEN input LIKE '%556e697377617056323a20504149525f45584953545300%' THEN 1 ELSE 0 END as has_pair_exists_error,
        
        -- Function selector detection
        CASE WHEN input LIKE '%017e7e58%' THEN 1 ELSE 0 END as has_fee_to_function,
        CASE WHEN input LIKE '%c9c65396%' THEN 1 ELSE 0 END as has_create_pair_function,
        CASE WHEN input LIKE '%1e3dd18b%' THEN 1 ELSE 0 END as has_get_pair_function,
        CASE WHEN input LIKE '%574f2ba3%' THEN 1 ELSE 0 END as has_all_pairs_length_function,
        CASE WHEN input LIKE '%a2e74af6%' THEN 1 ELSE 0 END as has_set_fee_to_function,
        CASE WHEN input LIKE '%e6a43905%' THEN 1 ELSE 0 END as has_all_pairs_function,
        
        -- Constructor pattern detection
        CASE WHEN input LIKE '0x608060405234801561001057600080fd5b50604051%' THEN 1 ELSE 0 END as has_constructor_pattern,
        
        -- EIP712 pattern detection
        CASE WHEN input LIKE '%454950373132446f6d61696e28737472696e67206e616d652c737472696e672076657273696f6e2c75696e74323536%' THEN 1 ELSE 0 END as has_eip712_pattern,
        
        -- Bytecode size analysis
        LENGTH(input) as bytecode_length
        
    FROM ethereum.core.fact_traces
    WHERE type = 'CREATE'
        AND trace_succeeded = TRUE
        AND input IS NOT NULL
        AND LENGTH(input) > 1000
        AND block_timestamp >= '2020-05-01'
        -- Pre-filter for performance
        AND (
            input LIKE '%556e697377617056323a20%' OR
            input LIKE '%017e7e58%' OR
            input LIKE '%c9c65396%'
        )
),

fork_similarity_analysis AS (
    SELECT 
        *,
        -- Calculate weighted similarity scores
        (has_forbidden_error + has_identical_error + has_zero_address_error + has_pair_exists_error) * 0.20 as error_score,
        (has_fee_to_function + has_create_pair_function + has_get_pair_function + has_all_pairs_length_function + has_set_fee_to_function + has_all_pairs_function) * 0.10 as function_score,
        (has_constructor_pattern + has_eip712_pattern) * 0.20 as structure_score,
        
        -- Total similarity score
        (has_forbidden_error + has_identical_error + has_zero_address_error + has_pair_exists_error) * 0.20 +
        (has_fee_to_function + has_create_pair_function + has_get_pair_function + has_all_pairs_length_function + has_set_fee_to_function + has_all_pairs_function) * 0.10 +
        (has_constructor_pattern + has_eip712_pattern) * 0.20 as total_similarity_score
        
    FROM contract_deployments
),

fork_classification AS (
    SELECT 
        *,
        CASE 
            WHEN total_similarity_score >= 1.6 THEN 'exact_fork'
            WHEN total_similarity_score >= 1.2 THEN 'modified_fork'
            WHEN total_similarity_score >= 0.8 THEN 'inspired_fork'
            WHEN total_similarity_score >= 0.4 THEN 'partial_match'
            ELSE 'unrelated'
        END as fork_classification,
        
        -- Identify modification patterns
        CASE 
            WHEN has_forbidden_error = 0 AND has_create_pair_function = 1 THEN 'modified_error_messages'
            WHEN has_constructor_pattern = 0 AND has_create_pair_function = 1 THEN 'modified_constructor'
            WHEN bytecode_length BETWEEN 45000 AND 55000 THEN 'similar_size_range'
            ELSE 'standard_patterns'
        END as modification_pattern,
        
        -- Calculate days since Uniswap V2 deployment
        DATEDIFF('day', '2020-05-05'::date, block_timestamp::date) as days_after_uniswap_v2
        
    FROM fork_similarity_analysis
    WHERE total_similarity_score >= 0.4
),

final_results AS (
    SELECT 
        f.*,
        t.from_address as transaction_sender,
        t.gas_used,
        t.gas_price,
        (t.gas_used * t.gas_price) / 1000000000000000000.0 as deployment_cost_eth
        
    FROM fork_classification f
    JOIN ethereum.core.fact_transactions t ON f.tx_hash = t.tx_hash
)

SELECT 
    tx_hash,
    block_timestamp,
    deployed_contract,
    deployer_address,
    fork_classification,
    ROUND(total_similarity_score, 3) as similarity_score,
    modification_pattern,
    days_after_uniswap_v2,
    ROUND(deployment_cost_eth, 6) as deployment_cost_eth,
    
    -- Pattern breakdown
    has_forbidden_error,
    has_identical_error,
    has_zero_address_error,
    has_pair_exists_error,
    has_create_pair_function,
    has_get_pair_function,
    has_constructor_pattern,
    has_eip712_pattern,
    
    -- Bytecode info
    bytecode_length,
    LEFT(bytecode, 100) as bytecode_preview,
    
    -- Scores breakdown
    ROUND(error_score, 3) as error_pattern_score,
    ROUND(function_score, 3) as function_pattern_score,
    ROUND(structure_score, 3) as structure_pattern_score

FROM final_results
WHERE fork_classification IN ('exact_fork', 'modified_fork', 'inspired_fork')
ORDER BY 
    total_similarity_score DESC,
    block_timestamp ASC