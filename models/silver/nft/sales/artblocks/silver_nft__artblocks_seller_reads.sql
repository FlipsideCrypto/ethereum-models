{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ["nft_address", "project_id"],
    cluster_by = ['nft_address'],
    tags = ['curated'],
    full_refresh = false
) }}
/*
Old artblocks contracts 
0x059edd72cd353df5106d2b9cc5ab83a52287ac3a
0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270

Current artblocks
0x99a9b7c1116f9ceeb1652de04d5969cce509b069

Artblocks collaborations
0x145789247973c5d612bf121e9e4eef84b63eb707
0xea698596b6009a622c3ed00dd5a8b5d1cae4fc36
0x64780ce53f6e966e18a22af13a2f97369580ec11

Artblocks explorations
0x942bc2d3e7a589fe5bd4a5c6ef9727dfd82f5c8a
*/
WITH inputs AS (

    SELECT
        nft_address,
        project_id,
        CONCAT(
            nft_address,
            project_id
        ) AS nft_address_id,
        block_number
    FROM
        {{ ref('silver_nft__artblocks_base_sales') }}
    WHERE
        nft_address IN (
            '0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270',
            '0x99a9b7c1116f9ceeb1652de04d5969cce509b069',
            '0x059edd72cd353df5106d2b9cc5ab83a52287ac3a',
            '0x145789247973c5d612bf121e9e4eef84b63eb707',
            '0xea698596b6009a622c3ed00dd5a8b5d1cae4fc36',
            '0x64780ce53f6e966e18a22af13a2f97369580ec11',
            '0x942bc2d3e7a589fe5bd4a5c6ef9727dfd82f5c8a'
        )

{% if is_incremental() %}
AND nft_address_id NOT IN (
    SELECT
        nft_address_id
    FROM
        {{ this }}
)
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY nft_address,
    project_id
    ORDER BY
        block_number ASC,
        event_index ASC
) = 2
),
build_requests AS (
    SELECT
        nft_address_id,
        nft_address,
        block_number,
        CASE
            WHEN nft_address IN (
                '0x059edd72cd353df5106d2b9cc5ab83a52287ac3a'
            ) THEN '0x8c2c3622' -- projectTokenInfo
            WHEN nft_address IN (
                '0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270',
                '0x99a9b7c1116f9ceeb1652de04d5969cce509b069',
                '0x145789247973c5d612bf121e9e4eef84b63eb707',
                '0xea698596b6009a622c3ed00dd5a8b5d1cae4fc36',
                '0x64780ce53f6e966e18a22af13a2f97369580ec11',
                '0x942bc2d3e7a589fe5bd4a5c6ef9727dfd82f5c8a'
            ) THEN '0xa47d29cb' --projectIdToArtistAddress
        END AS function_sig,
        project_id,
        CONCAT(
            function_sig,
            LPAD(
                REPLACE(LOWER(utils.udf_int_to_hex(project_id)), '0x', ''),
                64,
                0)
            ) AS DATA,
            utils.udf_json_rpc_call(
                'eth_call',
                [{ 'to': nft_address, 'from': null, 'data': data }, block_number]
            ) AS rpc_request,
            ROW_NUMBER() over (
                ORDER BY
                    nft_address_id ASC
            ) AS row_num
            FROM
                inputs qualify ROW_NUMBER() over (
                    ORDER BY
                        nft_address_id ASC
                ) <= 500
        ),
        requests AS ({% for item in range(10) %}
            (
        SELECT
            nft_address_id, nft_address, project_id, row_num, rpc_request, live.udf_api('POST', CONCAT('{service}', '/', '{Authentication}'),{}, rpc_request, 'Vault/prod/ethereum/quicknode/mainnet') AS resp
        FROM
            build_requests

{% if is_incremental() %}
WHERE
    row_num BETWEEN ({{ item }} * 50 + 1)
    AND ((({{ item }} + 1) * 50))
{% else %}
WHERE
    row_num BETWEEN ({{ item }} * 50 + 1)
    AND ((({{ item }} + 1) * 50))
{% endif %}) {% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %})
SELECT
    nft_address_id,
    nft_address,
    project_id,
    row_num,
    rpc_request,
    resp,
    resp :data :result :: STRING AS response,
    regexp_substr_all(SUBSTR(response, 3, len(response)), '.{64}') AS segmented_input,
    '0x' || SUBSTR(
        segmented_input [0] :: STRING,
        25
    ) AS artist_address,
    SYSDATE() AS _inserted_timestamp
FROM
    requests
WHERE
    response IS NOT NULL
    AND artist_address != '0x0000000000000000000000000000000000000000'
    AND artist_address IS NOT NULL
