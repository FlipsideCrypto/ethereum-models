{{ config(
    materialized = 'incremental',
    unique_key = 'proposal_id',
    full_refresh = false,
    tags = ['snapshot']
) }}

WITH recursive props_request AS (

    SELECT
        ethereum.streamline.udf_api(
            'GET',
            'https://hub.snapshot.org/graphql',{ 'apiKey':(
                SELECT
                    api_key
                FROM
                    {{ source(
                        'crosschain_silver',
                        'apis_keys'
                    ) }}
                WHERE
                    api_name = 'snapshot'
            ) },{ 'query': 'query { proposals(orderBy: "created", orderDirection: asc,first:1000,where:{created_gte: ' || max_time_start || '}) { id space{id voting {delay quorum period type}} created start end } }' }
        ) AS resp,
        SYSDATE() AS _inserted_timestamp,
        1000 AS total_retrieved,
        1000 AS records_to_retrieve
    FROM
        (
            SELECT
                DATE_PART(
                    epoch_second,
                    max_prop_start :: TIMESTAMP
                ) AS max_time_start
            FROM
                (

{% if is_incremental() %}
SELECT
    MAX(created_at) AS max_prop_start
FROM
    {{ this }}
{% else %}
SELECT
    0 AS max_prop_start
{% endif %}) max_time)
UNION ALL
SELECT
    ethereum.streamline.udf_api(
        'GET',
        'https://hub.snapshot.org/graphql',{ 'apiKey':(
            SELECT
                api_key
            FROM
                {{ source(
                    'crosschain_silver',
                    'apis_keys'
                ) }}
            WHERE
                api_name = 'snapshot'
        ) },{ 'query': 'query { proposals(orderBy: "created", orderDirection: asc,first: ' || r.records_to_retrieve || ', skip: ' || r.total_retrieved || ', where:{created_gte: ' || max_time_start || '}) { id space{id voting {delay quorum period type}} created start end } }' }
    ) AS resp,
    SYSDATE() AS _inserted_timestamp,
    r.total_retrieved + r.records_to_retrieve AS total_retrieved,
    r.records_to_retrieve
FROM
    props_request r
    JOIN (
        SELECT
            DATE_PART(
                epoch_second,
                max_prop_start :: TIMESTAMP
            ) AS max_time_start
        FROM
            (

{% if is_incremental() %}
SELECT
    MAX(created_at) AS max_prop_start
FROM
    {{ this }}
{% else %}
SELECT
    0 AS max_prop_start
{% endif %}) max_time)
ON 1 = 1
WHERE
    r.total_retrieved <= 5000
),
proposals_final AS (
    SELECT
        VALUE :id :: STRING AS proposal_id,
        VALUE :space :id :: STRING AS space_id,
        VALUE :space :voting :delay :: INTEGER AS delay,
        VALUE :space :voting :quorum :: INTEGER AS quorum,
        VALUE :space :voting :period :: INTEGER AS voting_period,
        VALUE :space :voting :type :: STRING AS voting_type,
        TO_TIMESTAMP_NTZ(
            VALUE :created
        ) AS prop_timestamp,
        TO_TIMESTAMP_NTZ(
            VALUE :start
        ) AS proposal_start_time,
        TO_TIMESTAMP_NTZ(
            VALUE :end
        ) AS proposal_end_time,
        VALUE,
        _inserted_timestamp
    FROM
        props_request,
        LATERAL FLATTEN(
            input => resp :data :data :proposals
        )
    WHERE
        space_id IS NOT NULL
        AND proposal_start_time IS NOT NULL
        AND proposal_end_time IS NOT NULL
        AND (
            delay IS NOT NULL
            OR quorum IS NOT NULL
            OR voting_period IS NOT NULL
            OR voting_type IS NOT NULL
        ) qualify(ROW_NUMBER() over (PARTITION BY proposal_id
    ORDER BY
        TO_TIMESTAMP_NTZ(VALUE :created) DESC)) = 1
)
SELECT
    proposal_id,
    space_id,
    delay / 3600 AS delay,
    quorum,
    voting_period / 3600 AS voting_period,
    voting_type,
    prop_timestamp AS created_at,
    proposal_start_time,
    proposal_end_time,
    _inserted_timestamp
FROM
    proposals_final
