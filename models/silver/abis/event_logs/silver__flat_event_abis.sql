{{ config (
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'contract_address',
    cluster_by = '_inserted_timestamp::date',
    tags = ['abis'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY (contract_address)"
) }}

WITH abi_base AS (

    SELECT
        contract_address,
        DATA,
        _inserted_timestamp
    FROM
        {{ ref('silver__abis') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '24 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
flat_abi AS (
    SELECT
        contract_address,
        _inserted_timestamp,
        DATA,
        VALUE :inputs AS inputs,
        VALUE :payable :: BOOLEAN AS payable,
        VALUE :stateMutability :: STRING AS stateMutability,
        VALUE :type :: STRING AS TYPE,
        VALUE :anonymous :: BOOLEAN AS anonymous,
        VALUE :name :: STRING AS NAME
    FROM
        abi_base,
        LATERAL FLATTEN (
            input => DATA
        )
    WHERE
        TYPE = 'event' qualify ROW_NUMBER() over (
            PARTITION BY contract_address,
            NAME,
            inputs
            ORDER BY
                LENGTH(inputs)
        ) = 1
),
event_types AS (
    SELECT
        contract_address,
        _inserted_timestamp,
        inputs,
        anonymous,
        NAME,
        ARRAY_AGG(
            VALUE :type :: STRING
        ) AS event_type
    FROM
        flat_abi,
        LATERAL FLATTEN (
            input => inputs
        )
    GROUP BY
        contract_address,
        _inserted_timestamp,
        inputs,
        anonymous,
        NAME
),
apply_udfs AS (
    SELECT
        contract_address,
        NAME AS event_name,
        PARSE_JSON(
            OBJECT_CONSTRUCT(
                'anonymous',
                anonymous,
                'inputs',
                inputs,
                'name',
                NAME,
                'type',
                'event'
            ) :: STRING
        ) AS abi,
        utils.udf_evm_text_signature(abi) AS simple_event_name,
        utils.udf_keccak256(simple_event_name) AS event_signature,
        NAME,
        inputs,
        event_type,
        _inserted_timestamp
    FROM
        event_types
)
SELECT
    contract_address,
    event_name,
    abi,
    simple_event_name,
    event_signature,
    NAME,
    inputs,
    event_type,
    _inserted_timestamp
FROM
    apply_udfs
