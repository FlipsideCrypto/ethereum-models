{{ config (
    materialized = "incremental",
    unique_key = "contract_address",
    merge_update_columns = ["contract_address"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(contract_address)",
    tags = ['abis']
) }}

WITH etherscan_abis AS (

    SELECT
        COALESCE(
            VALUE :"BLOCK_NUMBER" :: INT,
            VALUE :"block_number" :: INT
        ) AS block_number,
        COALESCE(
            VALUE :"CONTRACT_ADDRESS" :: STRING,
            VALUE :"contract_address" :: STRING
        ) AS contract_address,
        DATA,
        VALUE,
        'etherscan' AS abi_source,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_contract_abis') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
    AND IS_ARRAY(DATA)
    AND DATA <> '[]'
{% else %}
    {{ ref('bronze__streamline_fr_contract_abis') }}
WHERE
    IS_ARRAY(DATA)
    AND DATA <> '[]'
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY contract_address, block_number
ORDER BY
    _inserted_timestamp DESC)) = 1
),
user_abis AS (
    SELECT
        contract_address,
        abi,
        discord_username,
        _inserted_timestamp,
        'user' AS abi_source,
        abi_hash
    FROM
        {{ ref('silver__user_verified_abis') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
        WHERE
            abi_source = 'user'
    )
    AND contract_address NOT IN (
        SELECT
            contract_address
        FROM
            {{ this }}
    )
{% endif %}
),
all_abis AS (
    SELECT
        contract_address,
        DATA,
        _inserted_timestamp,
        abi_source,
        NULL AS discord_username,
        SHA2(DATA) AS abi_hash
    FROM
        etherscan_abis
    UNION
    SELECT
        contract_address,
        PARSE_JSON(abi) AS DATA,
        _inserted_timestamp,
        'user' AS abi_source,
        discord_username,
        abi_hash
    FROM
        user_abis
)
SELECT
    contract_address,
    DATA,
    _inserted_timestamp,
    abi_source,
    discord_username,
    abi_hash
FROM
    all_abis
WHERE
    DATA :: STRING <> 'Unknown Exception' qualify(ROW_NUMBER() over(PARTITION BY contract_address
ORDER BY
    _INSERTED_TIMESTAMP DESC)) = 1
