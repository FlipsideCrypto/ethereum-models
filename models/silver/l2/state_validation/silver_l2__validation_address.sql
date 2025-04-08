{{ config(
    materialized = 'table',
    unique_key = "state_block_number",
    tags = ['curated']
) }}

WITH validation_addresses AS (

    SELECT
        *
    FROM
        (
            VALUES
                (
                    'optimism',
                    'op_stack',
                    '0xe5965ab5962edc7477c8520243a95517cd252fa9',
                    'dispute_game'
                ),
                (
                    'optimism',
                    'op_stack',
                    LOWER('0xdfe97868233d1aa22e815a266982f2cf17685a27'),
                    'output_oracle'
                ),
                (
                    'optimism',
                    'op_stack',
                    LOWER('0xBe5dAb4A2e9cd0F27300dB4aB94BeE3A233AEB19'),
                    'legacy_state'
                ),
                (
                    'base',
                    'op_stack',
                    LOWER('0x43edB88C4B80fDD2AdFF2412A7BebF9dF42cB40e'),
                    'dispute_game'
                ),
                (
                    'ink',
                    'op_stack',
                    LOWER('0x10d7B35078d3baabB96Dd45a9143B94be65b12CD'),
                    'dispute_game'
                ),
                (
                    'bob',
                    'op_stack',
                    LOWER('0xdDa53E23f8a32640b04D7256e651C1db98dB11C1'),
                    'output_oracle'
                ),
                (
                    'boba',
                    'op_stack',
                    LOWER('0xbB7aD3f9CCbC94085b7F7B1D5258e59F5F068741'),
                    'output_oracle'
                ),
                (
                    'blast',
                    'op_stack',
                    LOWER('0x826D1B0D4111Ad9146Eb8941D7Ca2B6a44215c76'),
                    'output_oracle'
                ),
                (
                    'swell',
                    'op_stack',
                    LOWER('0x87690676786cDc8cCA75A472e483AF7C8F2f0F57'),
                    'dispute_game'
                )
        ) t (
            chain,
            chain_category,
            validation_address,
            validation_type
        )
)
SELECT
    chain,
    chain_category,
    validation_address,
    validation_type
FROM
    validation_addresses
