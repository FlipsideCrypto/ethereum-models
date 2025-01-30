{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    trace_index,
    utils.udf_hex_to_int(
        full_decoded_data :decoded_input_data :_data :: STRING
    ) :: STRING AS loanid,
    full_decoded_data :decoded_input_data :_to :: STRING AS mint_to_address,
    to_address AS obligation_receipt_address,
    full_decoded_data :decoded_input_data :_tokenId :: STRING AS tokenid,
    modified_timestamp AS _inserted_timestamp
FROM
    {{ ref('core__ez_decoded_traces') }}
WHERE
    block_timestamp :: DATE >= '2023-11-04'
    AND trace_succeeded
    AND tx_succeeded
    AND TYPE = 'CALL'
    AND to_address = '0xaabd3ebcc6ae1e87150c6184c038b94dc01a7708' -- obligation receipt
    AND full_decoded_data :function_name :: STRING = 'mint'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
