{{ config(
    materialized = 'incremental',
    unique_key = 'slot_number',
    tags = ['streamline_beacon_realtime']
) }}

WITH current_slot AS (

    SELECT
        slot_number AS max_slot
    FROM
        {{ ref("bronze__sepolia_slots") }}
    WHERE
        inserted_timestamp = (
            SELECT
                MAX(inserted_timestamp)
            FROM
                {{ ref("bronze__sepolia_slots") }}
        )
),
create_range AS (
    SELECT
        slot_number,
        ROW_NUMBER() over (
            ORDER BY
                slot_number
        ) AS row_no,
        CEIL(
            row_no / 2
        ) AS batch_no
    FROM
        (
            SELECT
                _id AS slot_number
            FROM
                {{ ref("silver__number_sequence") }}
            WHERE
                _id BETWEEN 4537722
                AND (
                    SELECT
                        max_slot
                    FROM
                        current_slot
                )

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
)
ORDER BY
    slot_number ASC
) {% for item in range(400) %}
SELECT
    slot_number,
    live.udf_api(
        CONCAT(
            '{Service}',
            '/',
            '{Authentication}',
            'eth/v1/beacon/blob_sidecars/',
            slot_number :: STRING
        ),
        'Vault/prod/ethereum/quicknode/sepolia'
    ) AS resp,
    SYSDATE() AS _inserted_timestamp
FROM
    create_range
WHERE
    batch_no = {{ item }} + 1 {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
