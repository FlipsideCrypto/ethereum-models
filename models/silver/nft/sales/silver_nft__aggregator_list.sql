{{ config(
    materialized = 'incremental',
    unique_key = 'aggregator_identifier',
    merge_update_columns = ['aggregator_identifier', 'aggregator', 'aggregator_type'],
    full_refresh = false,
    tags = ['stale']
) }}


WITH calldata_aggregators AS (
    SELECT
        *
    FROM
        (
            VALUES
                ('72db8c0b', 'Gem', 'calldata', '2022-09-20'),
                ('332d1229', 'Blur', 'calldata', '2022-10-20'),
                ('a8a9c101', 'Alpha Sharks', 'calldata', '2023-01-07'),
                ('61598d6d', 'Flip', 'calldata', '2022-10-14'),
                ('64617461', 'Rarible', 'calldata', '2022-08-23'),
                ('0e1c0c38', 'Magic Eden', 'calldata', '2024-03-07'),
                ('5d95ff4b', 'Mintify', 'calldata', '2024-03-19'),
                ('547220db', 'OKX', 'calldata', '2024-03-19')
        ) t (aggregator_identifier, aggregator, aggregator_type, _inserted_timestamp)
),

platform_routers as (
SELECT
        *
    FROM
        (
            VALUES
                ('0xf24629fbb477e10f2cf331c2b7452d8596b5c7a5', 'gem', 'router', '2022-01-09'),
                ('0x83c8f28c26bf6aaca652df1dbbe0e1b56f8baba2', 'gem', 'router', '2022-02-18'),
                ('0x39da41747a83aee658334415666f3ef92dd0d541', 'blur', 'router', '2022-07-26'),
                ('0x000000000000ad05ccc4f10045630fb830b95127', 'blur', 'router', '2022-10-19'),
                ('0x00000000005228b791a99a61f36a130d50600106', 'looksrare', 'router', '2023-03-02'),
               ('0x69cf8871f61fb03f540bc519dd1f1d4682ea0bf6', 'element', 'router', '2024-03-05'),
               ('0xb4e7b8946fa2b35912cc0581772cccd69a33000c', 'element', 'router', '2024-03-05'),
              ('0xef1c6e67703c7bd7107eed8303fbe6ec2554bf6b', 'uniswap', 'router', '2024-03-05'),
              ('0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad', 'uniswap', 'router', '2024-03-05')

        ) t (aggregator_identifier, aggregator, aggregator_type, _inserted_timestamp)
),

combined as (
SELECT * 
FROM
    calldata_aggregators

UNION ALL 

SELECT *
FROM
    platform_routers
)

SELECT 
    aggregator_identifier,
    aggregator, 
    aggregator_type,
    _inserted_timestamp
FROM combined

qualify row_number() over (partition by aggregator_identifier order by _inserted_timestamp desc ) = 1 