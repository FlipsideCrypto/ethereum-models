{{ config (
    materialized = "view",
    tags = ['streamline_beacon_complete']
) }}

SELECT
    _id AS slot_number
FROM
    {{ ref(
        'admin__number_sequence'
    ) }}
WHERE
    _id <= (
        SELECT
            COALESCE(
                slot_number,
                0
            )
        FROM
            {{ ref("streamline__get_beacon_chainhead") }}
    )
ORDER BY
    _id ASC -- height >= 4700013 -- Start slot for ETH 2.0 Beacon chain data
