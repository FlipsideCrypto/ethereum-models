{{ config(
    materialized = 'incremental',
    unique_key = 'slug_id',
    full_refresh = false
) }}

WITH spaces AS (
    SELECT
        ethereum.streamline.udf_api(
            'GET',
            'https://hub.snapshot.org/api/explore',{},{}
        ) AS resp
),

spaces_final AS (

SELECT
    key AS slug_id,
    VALUE :categories AS categories_array,
    VALUE :followers :: INTEGER AS followers,
    VALUE :followers_7d :: INTEGER AS followers_7d,
    VALUE :name :: STRING AS NAME,
    VALUE :network :: STRING AS chain_id,
    VALUE :networks AS chain_id_array
FROM
    spaces,
    LATERAL FLATTEN(
        input => resp :data :spaces
    )
{% if is_incremental() %}
WHERE
    slug_id NOT IN (
        SELECT
            slug_id
        FROM
            {{ this }}
    )
{% endif %}
)

SELECT
    slug_id,
    categories_array,
    followers,
    followers_7d,
    NAME,
    chain_id,
    chain_id_array
FROM spaces_final

