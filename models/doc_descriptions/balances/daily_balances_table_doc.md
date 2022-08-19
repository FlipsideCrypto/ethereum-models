{% docs daily_balances_table_doc %}

READ THIS!

TLDR - You MUST use filters when interacting with this view. At a minimum, the `balance_date` must be filtered. Use `ez_balance_diffs` if possible. Only activity 2020+ is included.

This view contains the average daily balance of every wallet, for every token, including ERC721s. Previous balances are carried forward on days without activity.

This is an absolutely massive view, which is why filters must be applied if you do not want your query to time out. Balance_date is a requirement, but other filters will also be helpful for query performance. Data before 2020 has been excluded for performance sake. Wallets with activity before Jan 1 2020 may be represented incorrectly. You can find these older records in `ez_balance_diffs`.

`ez_balance_diffs` will have the best query performance of the balances tables, please use it if possible.

{% enddocs %}