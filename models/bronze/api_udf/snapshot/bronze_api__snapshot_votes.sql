{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    full_refresh = false,
    tags = ['snapshot']
) }}

WITH recursive votes_request AS (

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
            ) },{ 'query': 'query { votes(orderBy: "created", orderDirection: asc, first: 1000, where:{created_gte: ' || max_time_start || '}) { id proposal{id} ipfs voter created choice vp } }' }
        ) AS resp,
        SYSDATE() AS _inserted_timestamp,
        1000 AS total_retrieved,
        1000 AS records_to_retrieve
    FROM
        (
            SELECT
                DATE_PART(
                    epoch_second,
                    max_vote_start :: TIMESTAMP
                ) AS max_time_start
            FROM
                (

{% if is_incremental() %}
SELECT
    MAX(vote_timestamp) AS max_vote_start
FROM
    {{ this }}
{% else %}
SELECT
    0 AS max_vote_start
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
        ) },{ 'query': 'query { votes(orderBy: "created", orderDirection: asc, first: ' || r.records_to_retrieve || ', skip: ' || r.total_retrieved || ', where:{created_gte: ' || max_time_start || '}) { id proposal{id} ipfs voter created choice vp } }' }
    ) AS resp,
    SYSDATE() AS _inserted_timestamp,
    r.total_retrieved + r.records_to_retrieve AS total_retrieved,
    r.records_to_retrieve
FROM
    votes_request r
    JOIN (
        SELECT
            DATE_PART(
                epoch_second,
                max_vote_start :: TIMESTAMP
            ) AS max_time_start
        FROM
            (

{% if is_incremental() %}
SELECT
    MAX(vote_timestamp) AS max_vote_start
FROM
    {{ this }}
{% else %}
SELECT
    0 AS max_vote_start
{% endif %}) max_time)
ON 1 = 1
WHERE
    r.total_retrieved <= 5000
),
votes_final AS (
    SELECT
        SPLIT(
            VALUE :choice :: STRING,
            ';'
        ) AS vote_option,
        VALUE :id :: STRING AS id,
        VALUE :ipfs :: STRING AS ipfs,
        VALUE :proposal :id :: STRING AS proposal_id,
        VALUE :voter :: STRING AS voter,
        VALUE :vp :: NUMBER AS voting_power,
        TO_TIMESTAMP_NTZ(
            VALUE :created
        ) AS vote_timestamp,
        _inserted_timestamp
    FROM
        votes_request,
        LATERAL FLATTEN(
            input => resp :data :data :votes
        ) qualify(ROW_NUMBER() over(PARTITION BY id
    ORDER BY
        TO_TIMESTAMP_NTZ(VALUE :created) DESC)) = 1
)
SELECT
    id,
    ipfs,
    proposal_id,
    voter,
    voting_power,
    vote_timestamp,
    vote_option,
    _inserted_timestamp
FROM
    votes_final
    INNER JOIN {{ ref('bronze_api__snapshot_proposals') }} USING (proposal_id)
