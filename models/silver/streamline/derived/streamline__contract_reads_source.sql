{{ config(
    materialized = 'view',
    tags = ['streamline_view']
) }}

SELECT 
    CONTRACT_ADDRESS,
    CASE (
        WHEN FUNCTION_INPUT IS NULL THEN FUNCTION_SIGNATUE
        WHEN STARTSWITH(FUNCTION_INPUT, '0x') THEN LTRIM(FUNCTION_INPUT, 2)
        ELSE FUNCTION_INPUT
    ) AS INPUT_DATA,
    CALL_NAME,
    IFF(
        TYPEOF(BLOCK_NUMBER) = 'STRING', 
        BLOCK_NUMBER, 
        HEX_ENCODE(BLOCK_NUMBER)
    ) as BLOCK_NUMBER_HEX
FROM 
    {{ref('streamline__contract_reads')}}