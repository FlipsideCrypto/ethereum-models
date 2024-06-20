{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'registry_metadata_id',
    full_refresh = false,
    tags = ['curated']
) }}

WITH new_records AS (

    SELECT
        block_number,
        contract_address,
        function_input AS registry_id,
        token_uri_link,
        _inserted_timestamp,
        ROW_NUMBER() over (
            ORDER BY
                contract_address,
                registry_id
        ) AS row_num
    FROM
        {{ ref('silver_olas__registry_reads') }}

{% if is_incremental() %}
WHERE
    (
        _inserted_timestamp > (
            SELECT
                MAX(_inserted_timestamp)
            FROM
                {{ this }}
        )
        AND CONCAT(
            contract_address,
            '-',
            registry_id
        ) NOT IN (
            SELECT
                CONCAT(
                    contract_address,
                    '-',
                    registry_id
                )
            FROM
                {{ this }}
        )
    )
    OR CONCAT(
        contract_address,
        '-',
        registry_id
    ) IN (
        SELECT
            CONCAT(
                contract_address,
                '-',
                registry_id
            )
        FROM
            {{ this }}
        WHERE
            NAME IS NULL
    )
{% endif %}
),
uri_calls AS (
    SELECT
        block_number,
        contract_address,
        registry_id,
        token_uri_link,
        live.udf_api(token_uri_link) AS resp,
        _inserted_timestamp
    FROM
        new_records
    WHERE
        row_num <= 100
    UNION ALL
    SELECT
        block_number,
        contract_address,
        registry_id,
        token_uri_link,
        live.udf_api(token_uri_link) AS resp,
        _inserted_timestamp
    FROM
        new_records
    WHERE
        row_num > 100
        AND row_num <= 200
    UNION ALL
    SELECT
        block_number,
        contract_address,
        registry_id,
        token_uri_link,
        live.udf_api(token_uri_link) AS resp,
        _inserted_timestamp
    FROM
        new_records
    WHERE
        row_num > 200
),
response AS (
    SELECT
        resp,
        block_number,
        contract_address,
        registry_id,
        token_uri_link,
        resp :data :attributes [0] :trait_type :: STRING AS trait_type,
        resp :data :attributes [0] :value :: STRING AS trait_value,
        REPLACE(
            resp :data :code_uri :: STRING,
            'ipfs://',
            'https://gateway.autonolas.tech/ipfs/'
        ) AS code_uri_link,
        resp :data :description :: STRING AS description,
        CASE
            WHEN resp :data :image :: STRING ILIKE 'ipfs://%' THEN REPLACE(
                resp :data :image :: STRING,
                'ipfs://',
                'https://gateway.autonolas.tech/ipfs/'
            )
            WHEN resp :data :image :: STRING NOT ILIKE '%://%' THEN CONCAT(
                'https://gateway.autonolas.tech/ipfs/',
                resp :data :image :: STRING
            )
            ELSE resp :data :image :: STRING
        END AS image_link,
        resp :data :name :: STRING AS NAME,
        _inserted_timestamp
    FROM
        uri_calls
)
SELECT
    resp,
    block_number,
    contract_address,
    registry_id,
    token_uri_link,
    trait_type,
    trait_value,
    code_uri_link,
    description,
    image_link,
    NAME,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address','registry_id']
    ) }} AS registry_metadata_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    response
WHERE
    resp :: STRING NOT ILIKE '%merkledag: not found%'
    AND resp :: STRING NOT ILIKE '%tuple index out of range%'
    AND resp :: STRING NOT ILIKE '%"error":%'
