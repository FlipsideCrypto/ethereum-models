{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    full_refresh = false
) }}

WITH proposals_array AS (
    SELECT 
        CONCAT('[',LISTAGG(CONCAT('"',proposal_id,'"'),', '),']') AS all_proposals
    FROM
        {{ ref('bronze_api__snapshot_proposals') }}
),

max_time AS (
    {% if is_incremental() %}
    SELECT
        MAX(vote_start_time) AS max_vote_start
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
            'query { votes(orderBy: "created", orderDirection: asc,first:20,where:{proposal_in: ',
            all_proposals,
            ', created_gt: ',
            max_time_start,
            '}) { id proposal{id} ipfs voter created choice vp } }'
        ) AS vote_created
    FROM
        proposals_array
    JOIN (
        SELECT 
            DATE_PART(epoch_second, max_vote_start::TIMESTAMP) AS max_time_start
        FROM 
            max_time
    ) ON 1=1 
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