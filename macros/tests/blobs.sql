{% test blobs_behind(
    model,
    threshold
) %}
WITH slots AS (
    SELECT
        MAX(slot_number) AS max_slot
    FROM
        {{ ref('silver__beacon_blocks') }}
),
blobs AS (
    SELECT
        MAX(slot_number) AS max_blob_slot
    FROM
        {{ model }}
),
difference AS (
    SELECT
        max_slot,
        max_blob_slot,
        max_slot - max_blob_slot AS slots_behind
    FROM
        slots
        JOIN blobs
        ON 1 = 1
)
SELECT
    slots_behind
FROM
    difference
WHERE
    slots_behind > {{ threshold }}

    {% endtest %}
