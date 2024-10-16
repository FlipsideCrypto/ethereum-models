{{ config(
    materialized = 'incremental',
    unique_key = 'slot_number',
    full_refresh = false,
    tags = ['streamline_beacon_realtime']
) }}

WITH slot_range AS (

    SELECT
        slot_number
    FROM
        {{ ref("streamline__beacon_blocks") }}
    WHERE
        slot_number >= 8626178 -- EIP-4844 fork slot

{% if is_incremental() %}
EXCEPT
SELECT
    slot_number
FROM
    {{ this }}
WHERE
    LEFT(
        resp :error :: STRING,
        1
    ) <> 'F'
    OR resp :error IS NULL
{% endif %}
),
create_range AS (
    SELECT
        slot_number,
        ROW_NUMBER() over (
            ORDER BY
                slot_number
        ) AS row_no,
        CEIL(
            row_no / 3
        ) AS batch_no
    FROM
        slot_range
    ORDER BY
        slot_number ASC
) {% for item in range(600) %}
SELECT
    slot_number,
    live.udf_api(
        CONCAT(
            '{service}',
            '/',
            '{Authentication}',
            '/eth/v1/beacon/blob_sidecars/',
            slot_number :: STRING
        ),
        'Vault/prod/ethereum/quicknode/mainnet'
    ) AS resp,
    SYSDATE() AS _inserted_timestamp
FROM
    create_range
WHERE
    batch_no = {{ item }} + 1 {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
