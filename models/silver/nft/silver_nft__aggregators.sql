{{ config(
    materialized = 'incremental',
    unique_key = "hash_for_join"
) }}
--credits to hildobby and 0xRob for reservoir calldata logic https://github.com/duneanalytics/spellbook/blob/main/models/nft/ethereum/nft_ethereum_aggregators_markers.sql
{# WITH nft_sales_filter AS (

SELECT
    DISTINCT origin_to_address
FROM
    {{ ref('core__ez_nft_sales') }}
WHERE
    block_timestamp >= '2022-07-01'

{% if is_incremental() %}
AND block_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE
    FROM
        {{ this }}
)
{% endif %}

#}
WITH raw_calldata AS (
    SELECT
        DISTINCT REGEXP_REPLACE(
            input_data,
            '^.*00',
            ''
        ) AS calldata_hash
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        block_timestamp >= '2022-07-01' {# AND to_address IN (
    SELECT
        origin_to_address
    FROM
        nft_sales_filter
) #}
{# AND to_address IN (
'0x00000000006c3852cbef3e08e8df289169ede581',
--seaport
'0x74312363e45dcaba76c59ec49a7aa8a65a67eed3',
--x2y2
'0x59728544b08ab483533076417fbbb2fd0b17ce3a',
--looksrare
'0x9ebfb53fa8526906738856848a27cb11b0285c3f'
) #}
AND RIGHT(
    input_data,
    2
) = '1f'
AND LEFT(REGEXP_REPLACE(input_data, '^.*00', ''), 2) = '1f'
AND REGEXP_REPLACE(
    input_data,
    '^.*00',
    ''
) != '1f'
AND LENGTH(REGEXP_REPLACE(input_data, '^.*00', '')) % 2 = 0
AND tx_success = TRUE

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE
    FROM
        {{ this }}
)
{% endif %}
),
hash_identifiers AS (
    SELECT
        calldata_hash,
        SPLIT(
            calldata_hash,
            '1f'
        ) [1] :: STRING AS hash_for_decode,
        TRY_HEX_DECODE_STRING(hash_for_decode) AS marketplace_decoded,
        hash_for_decode || '1f' AS hash_for_join,
        LENGTH(hash_for_join) AS hash_for_join_length
    FROM
        raw_calldata
    WHERE
        marketplace_decoded IS NOT NULL
        AND calldata_hash != '1f1f'
        AND ARRAY_SIZE(SPLIT(calldata_hash, '1f')) = 3
),
known_domain_hash AS (
    SELECT
        *
    FROM
        (
            VALUES
                (
                    NULL,
                    NULL,
                    'Gem',
                    '72db8c0b',
                    8
                ),
                (
                    NULL,
                    NULL,
                    'Blur',
                    '332d1229',
                    8
                ),
                (
                    NULL,
                    NULL,
                    'Alpha Sharks',
                    'a8a9c101',
                    8
                ),
                (
                    NULL,
                    NULL,
                    'Flip',
                    '61598d6d',
                    8
                ),
                (
                    NULL,
                    NULL,
                    'Rarible',
                    '9616c6c64617461',
                    15
                )
        ) t (
            calldata_hash,
            hash_for_decode,
            marketplace_decoded,
            hash_for_join,
            hash_for_join_length
        )
),
all_domain AS (
    SELECT
        *,
        SYSDATE() AS _inserted_timestamp
    FROM
        hash_identifiers
    UNION ALL
    SELECT
        *,
        SYSDATE() AS _inserted_timestamp
    FROM
        known_domain_hash
)
SELECT
    calldata_hash,
    hash_for_decode,
    marketplace_decoded,
    hash_for_join,
    hash_for_join_length,
    _inserted_timestamp
FROM
    all_domain
