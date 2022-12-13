{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    full_refresh = false
) }}

WITH votes_historical AS (

    SELECT
        id,
        ipfs,
        proposal_id,
        voter,
        voting_power,
        vote_timestamp,
        vote_option,
        _inserted_timestamp
    FROM {{ ref('bronze_api__snapshot_votes_historical') }}
{% if is_incremental() %}
WHERE _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
    )
{% endif %}
),

max_time AS (
    {% if is_incremental() %}
    SELECT
        MAX(vote_timestamp) AS max_vote_start
    FROM
        {{ this }}
    {% else %}
    SELECT
        0 AS max_vote_start
    {% endif %}
),

ready_votes AS (
    SELECT
        CONCAT(
            'query { votes(orderBy: "created", orderDirection: asc,first:1000,where:{created_gte: ',
            max_time_start,
            '}) { id proposal{id} ipfs voter created choice vp } }'
        ) AS vote_created
    FROM(
        SELECT 
            DATE_PART(epoch_second, max_vote_start::TIMESTAMP) AS max_time_start
        FROM 
            max_time
    )
),

vote_data_created AS (
    SELECT
        ethereum.streamline.udf_api(
            'GET',
            'https://hub.snapshot.org/graphql',{},{ 'query': vote_created }
        ) AS resp,
        SYSDATE() AS _inserted_timestamp
    FROM
        ready_votes
),

votes_final AS (
    
SELECT
    SPLIT(VALUE :choice :: STRING, ';') AS vote_option,
    VALUE :id :: STRING AS id,
    VALUE :ipfs :: STRING AS ipfs, 
    VALUE :proposal :id :: STRING AS proposal_id, 
    VALUE :voter :: STRING AS voter, 
    VALUE :vp :: NUMBER AS voting_power, 
    TO_TIMESTAMP_NTZ(VALUE :created) AS vote_timestamp,
    _inserted_timestamp
FROM vote_data_created,
LATERAL FLATTEN(
    input => resp :data :data :votes
)
WHERE id NOT IN (
    SELECT
        DISTINCT id
    FROM
        votes_historical
    )
    AND proposal_id IS NOT NULL
QUALIFY(ROW_NUMBER() over(PARTITION BY id
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
FROM votes_final
UNION
SELECT
    id,
    ipfs,
    proposal_id,
    voter,
    voting_power,
    vote_timestamp,
    vote_option,
    _inserted_timestamp
FROM votes_historical