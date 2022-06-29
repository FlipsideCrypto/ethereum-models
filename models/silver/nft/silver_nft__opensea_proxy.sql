{{ config(
    materialized = 'incremental',
    unique_key = 'wyvern_proxy_address',
    cluster_by = ['ingested_at::DATE']
) }}

SELECT
    DISTINCT to_address :: STRING AS wyvern_proxy_address,
    CASE
        WHEN from_address = LOWER('0xa5409ec958C83C3f309868babACA7c86DCB077c1') THEN 'opensea'
    END AS creator,
    ingested_at
FROM
    {{ ref('silver__traces') }}
WHERE
    from_address = LOWER('0xa5409ec958C83C3f309868babACA7c86DCB077c1')
    AND TYPE = 'CREATE'

{% if is_incremental() %}
AND ingested_at >= (
    SELECT
        MAX(
            ingested_at
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
