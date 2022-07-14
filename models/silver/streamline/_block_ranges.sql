{{ config (
    materialized = "ephemeral",
    unique_key = "block_number",
) }}

SELECT
    block_number,
    CASE
        WHEN RIGHT(
            block_number,
            1
        ) = 0 THEN block_number
    END AS block_number_10,
    CASE
        WHEN RIGHT(
            block_number,
            2
        ) IN (
            00,
            25,
            50,
            75
        ) THEN block_number
    END AS block_number_25,
    CASE
        WHEN RIGHT(
            block_number,
            2
        ) IN (
            00,
            50
        ) THEN block_number
    END AS block_number_50,
    CASE
        WHEN RIGHT(
            block_number,
            2
        ) IN (00) THEN block_number
    END AS block_number_100,
    CASE
        WHEN RIGHT(
            block_number,
            3
        ) IN (000) THEN block_number
    END AS block_number_1000,
    CASE
        WHEN RIGHT(
            block_number,
            4
        ) IN (0000) THEN block_number
    END AS block_number_10000,
    _inserted_timestamp
FROM
    {{ ref("silver__blocks") }}
