{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_get_beacon_txs()",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

SELECT
    {{ dbt_utils.surrogate_key(
        ['tx_hash']
    ) }} AS id,
    tx_hash
FROM
    {{ ref("streamline__beacon_txs") }}
WHERE
    tx_hash IS NOT NULL
EXCEPT
SELECT
    id,
    tx_hash
FROM
    {{ ref("streamline__complete_beacon_txs") }}
UNION ALL
SELECT
    id,
    tx_hash
FROM
    {{ ref("streamline__beacon_txs_history") }}
