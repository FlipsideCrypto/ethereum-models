{{ config (
    materialized = "incremental",
    unique_key = "contract_address",
    merge_update_columns = ["contract_address"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(contract_address)"
) }}

WITH meta AS (

    SELECT
        registered_on,
        last_modified,
        LEAST(
            last_modified,
            registered_on
        ) AS _inserted_timestamp,
        file_name
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "bronze_streamline", "contract_abis") }}'
            )
        ) A

{% if is_incremental() %}
WHERE
    LEAST(
        registered_on,
        last_modified
    ) >= (
        SELECT
            COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
        FROM
            {{ this }}
        WHERE
            abi_source = 'etherscan')
    )
{% else %}
)
{% endif %},
etherscan_abis AS (
    SELECT
        contract_address,
        block_number,
        DATA,
        _INSERTED_TIMESTAMP,
        metadata,
        VALUE,
        'etherscan' AS abi_source
    FROM
        {{ source(
            "bronze_streamline",
            "contract_abis"
        ) }}
        JOIN meta m
        ON m.file_name = metadata $ filename
    WHERE
        DATA :: STRING <> 'Contract source code not verified' qualify(ROW_NUMBER() over(PARTITION BY contract_address
    ORDER BY
        block_number DESC, _INSERTED_TIMESTAMP DESC)) = 1
),
user_abis AS (
    SELECT
        contract_address,
        abi,
        discord_username,
        _inserted_timestamp,
        'user' AS abi_source
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
{% endif %}
),
all_abis AS (
    SELECT
        contract_address,
        DATA,
        _inserted_timestamp,
        abi_source,
        NULL AS discord_username
    FROM
        etherscan_abis
    UNION
    SELECT
        contract_address,
        PARSE_JSON(abi) AS DATA,
        _inserted_timestamp,
        'user' AS abi_source,
        discord_username
    FROM
        user_abis
)
SELECT
    *
FROM
    all_abis qualify(ROW_NUMBER() over(PARTITION BY contract_address
ORDER BY
    _INSERTED_TIMESTAMP DESC)) = 1
