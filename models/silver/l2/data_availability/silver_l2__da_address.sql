{{ config(
    materialized = 'table',
    unique_key = ['chain', 'da_address'],
    tags = ['curated']
) }}

WITH da_addresses AS (

    SELECT
        *
    FROM
        (
            VALUES
                (
                    'optimism',
                    'op_stack',
                    LOWER('0x5E4e65926BA27467555EB562121fac00D24E9dD2'),
                    'calldata'
                ),
                (
                    'optimism',
                    'op_stack',
                    LOWER('0xFF00000000000000000000000000000000000010'),
                    'blobs'
                ),
                (
                    'boba',
                    'op_stack',
                    LOWER('0xfBd2541e316948B259264c02f370eD088E04c3Db'),
                    'calldata'
                ),
                (
                    'arbitrum',
                    'arbitrum',
                    LOWER('0x4c6f947Ae67F572afa4ae0730947DE7C874F95Ef'),
                    'calldata'
                ),
                (
                    'arbitrum',
                    'arbitrum',
                    LOWER('0x1c479675ad559DC151F6Ec7ed3FbF8ceE79582B6'),
                    'calldata'
                ),
                (
                    'bob',
                    'op_stack',
                    LOWER('0x3A75346f81302aAc0333FB5DCDD407e12A6CfA83'),
                    'blobs'
                ),
                (
                    'swell',
                    'op_stack',
                    LOWER('0x005dE5857e38dFD703a1725c0900E9C6f24cbdE0'),
                    'blobs'
                ),
                (
                    'base',
                    'op_stack',
                    LOWER('0xFf00000000000000000000000000000000008453'),
                    'blobs'
                ),
                (
                    'blast',
                    'op_stack',
                    LOWER('0xFf00000000000000000000000000000000081457'),
                    'blobs'
                ),
                (
                    'ink',
                    'op_stack',
                    LOWER('0x005969bf0EcbF6eDB6C47E5e94693b1C3651Be97'),
                    'blobs'
                ),
                (
                    'scroll',
                    'zk_rollup',
                    LOWER('0xa13BAF47339d63B743e7Da8741db5456DAc1E556'),
                    'blobs'
                )
        ) t (
            chain,
            chain_category,
            da_address,
            submission_type
        )
)
SELECT
    chain,
    chain_category,
    da_address,
    submission_type
FROM
    da_addresses
