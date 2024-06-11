WITH to_subquery AS (
    SELECT
        TO AS to_address,
        "from" AS counterparty_address,
        evt_block_time,
        SUM(
            VALUE / 1e18
        ) AS to_amount
    FROM
        uniswap_v2_ethereum.pair_evt_transfer
    WHERE
        contract_address = '0x09D1d767eDF8Fa23A64C51fa559E0688E526812F'
        AND (
            TO = '0xa0DA53447C0f6C4987964d8463da7e6628B30f82'
            OR TO = '0x87cc0d34f6111c8A7A4Bdf758a9a715A3675f941'
        )
    GROUP BY
        TO,
        "from",
        evt_block_time
),
from_subquery AS (
    SELECT
        "from" AS from_address,
        SUM(
            VALUE / 1e18
        ) AS from_amount
    FROM
        uniswap_v2_ethereum.pair_evt_transfer
    WHERE
        contract_address = '0x09D1d767eDF8Fa23A64C51fa559E0688E526812F'
        AND (
            "from" = '0xa0DA53447C0f6C4987964d8463da7e6628B30f82'
            OR "from" = '0x87cc0d34f6111c8A7A4Bdf758a9a715A3675f941'
        )
    GROUP BY
        "from"
),
daily_results AS (
    SELECT
        to_subquery.to_address AS address,
        to_subquery.counterparty_address,
        SUM(
            to_subquery.to_amount
        ) - COALESCE(SUM(from_subquery.from_amount), 0) AS added_value
    FROM
        to_subquery
        LEFT JOIN from_subquery
        ON to_subquery.to_address = from_subquery.from_address
    GROUP BY
        to_subquery.to_address,
        to_subquery.counterparty_address
)
SELECT
    counterparty_address,
    CASE
        WHEN address = '0xa0DA53447C0f6C4987964d8463da7e6628B30f82' THEN 'Bonding Program'
        ELSE ''
    END AS address,
    ROUND(SUM(added_value), 2) AS lp_tokens_added
FROM
    daily_results
WHERE
    counterparty_address != '0x0000000000000000000000000000000000000000'
GROUP BY
    counterparty_address,
    address
ORDER BY
    lp_tokens_added DESC;
