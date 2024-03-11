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
        _id AS slot_number,
        ROW_NUMBER() over (
            ORDER BY
                _id
        ) AS row_no,
        CEIL(
            row_no / 10
        ) AS batch_no
    FROM
        {{ ref("silver__number_sequence") }}
        JOIN current_slot
        ON max_slot >= _id
    WHERE
        _id >= 4537722 -- starting point, can be changed

{% if is_incremental() %}
AND _id NOT IN (
    SELECT
        DISTINCT slot_number
    FROM
        {{ this }}
)
{% endif %}
) {% for item in range(800) %}
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
