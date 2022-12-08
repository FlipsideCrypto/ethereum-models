WITH spaces AS (
    SELECT
        ethereum.streamline.udf_api(
            'GET',
            'https://hub.snapshot.org/api/explore',{},{}
        ) AS resp
),

spaces_data AS (
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
),

flat_spaces AS (
    SELECT
        *,
        DATE_PART(epoch_second, CURRENT_DATE-1) AS plug_max_time --replace with MAX(timestamp) from silver_snapshot(historical) table
    FROM
        spaces_data
),

group_slugs AS (  
    SELECT 
        CONCAT('[',LISTAGG(CONCAT('"',slug_id,'"'),', '),']') AS all_spaces
    FROM flat_spaces
),

ready_prop_requests AS (
    SELECT
        CONCAT(
            'query { proposals(orderBy: "created", orderDirection: asc,first:20000,where:{space_in: ',
            all_spaces,
            ', created_gt: ',
            plug_max_time,
            '}) { id space{id} ipfs author created network type title body start end state votes choices scores_state scores } }'
        ) AS proposal_request_created,
        CONCAT(
            'query { proposals(orderBy: "created", orderDirection: asc,first:20000,where:{space_in: ',
            all_spaces,
            ', end_gt: ',
            plug_max_time,
            '}) { id space{id} ipfs author created network type title body start end state votes choices scores_state scores } }'
        ) AS proposal_request_end
    FROM
        group_slugs
    JOIN (SELECT DISTINCT plug_max_time FROM flat_spaces) --replace with MAX(timestamp) from silver_snapshot(historical) table 
        ON 1=1
),

proposal_data_created AS (
    SELECT
        ethereum.streamline.udf_api(
            'GET',
            'https://hub.snapshot.org/graphql',{},{ 'query': proposal_request_created }
        ) AS resp,
        SYSDATE() AS _inserted_timestamp
    FROM
        ready_prop_requests
),

proposal_data_end AS (
    SELECT
        ethereum.streamline.udf_api(
            'GET',
            'https://hub.snapshot.org/graphql',{},{ 'query': proposal_request_end }
        ) AS resp,
        SYSDATE() AS _inserted_timestamp
    FROM
        ready_prop_requests
),

proposals_final AS (
    
SELECT
    VALUE :id :: STRING AS proposal_id, 
    VALUE :ipfs :: STRING AS ipfs, 
    STRTOK_TO_ARRAY(VALUE :choices, ';') AS choices, 
    VALUE :author :: STRING AS proposal_author, 
    VALUE :title :: STRING AS proposal_title, 
    VALUE :body :: STRING AS proposal_text, 
    VALUE :space :id :: STRING AS space_id, 
    TO_TIMESTAMP_NTZ(VALUE :start) AS proposal_start_time, 
    TO_TIMESTAMP_NTZ(VALUE :end) AS proposal_end_time,
    VALUE
FROM
    proposal_data_created,
LATERAL FLATTEN(
    input => resp :data :data :proposals
)
-- WHERE 
--     VALUE:key LIKE '%govy-props%'
--     AND proposal_author IS NOT NULL
--     AND proposal_text IS NOT NULL
--     AND proposal_end_time IS NOT NULL
--     AND proposal_start_time IS NOT NULL 
--     AND space_id IS NOT NULL
--     AND proposal_title IS NOT NULL 
--     AND choices IS NOT NULL
QUALIFY(ROW_NUMBER() over(PARTITION BY proposal_id
  ORDER BY
    TO_TIMESTAMP_NTZ(VALUE :created) DESC)) = 1
UNION ALL
SELECT
    VALUE :id :: STRING AS proposal_id, 
    VALUE :ipfs :: STRING AS ipfs, 
    STRTOK_TO_ARRAY(VALUE :choices, ';') AS choices, 
    VALUE :author :: STRING AS proposal_author, 
    VALUE :title :: STRING AS proposal_title, 
    VALUE :body :: STRING AS proposal_text, 
    VALUE :space :id :: STRING AS space_id, 
    TO_TIMESTAMP_NTZ(VALUE :start) AS proposal_start_time, 
    TO_TIMESTAMP_NTZ(VALUE :end) AS proposal_end_time,
    VALUE
FROM
    proposal_data_end,
LATERAL FLATTEN(
    input => resp :data :data :proposals
)
-- WHERE 
--     VALUE:key LIKE '%govy-props%'
--     AND proposal_author IS NOT NULL
--     AND proposal_text IS NOT NULL
--     AND proposal_end_time IS NOT NULL
--     AND proposal_start_time IS NOT NULL 
--     AND space_id IS NOT NULL
--     AND proposal_title IS NOT NULL 
--     AND choices IS NOT NULL
QUALIFY(ROW_NUMBER() over(PARTITION BY proposal_id
  ORDER BY
    TO_TIMESTAMP_NTZ(VALUE :end) DESC)) = 1 --confirm end vs created in qualify statement
),

------------------------

group_proposals AS (  
    SELECT 
        CONCAT('[',LISTAGG(CONCAT('"',proposal_id,'"'),', '),']') AS all_proposals --determine where to pull proposal_ids from historically
    FROM proposals_final
),

ready_votes AS (
    SELECT
        CONCAT(
            'query { votes(orderBy: "created", orderDirection: asc,first:20000,where:{proposal_in: ',
            all_proposals,
            ', created_gt: ',
            plug_max_time,
            '}) { id proposal{id} ipfs voter created choice vp } }'
        ) AS vote_created
    FROM
        group_proposals
    JOIN (SELECT DISTINCT plug_max_time FROM flat_spaces) --replace with MAX(timestamp) from silver_snapshot(historical) table 
        ON 1=1
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
)

SELECT
    SPLIT(VALUE :choice :: STRING, ';') AS vote_option,
    VALUE :id :: STRING AS id,
    VALUE :ipfs :: STRING AS ipfs, 
    VALUE :prop_id :: STRING AS proposal_id, 
    VALUE :voter :: STRING AS voter, 
    VALUE :vp :: NUMBER AS voting_power, 
    TO_TIMESTAMP_NTZ(VALUE :created) AS vote_timestamp
FROM vote_data_created,
LATERAL FLATTEN(
    input => resp :data :data :votes
)
QUALIFY(ROW_NUMBER() over(PARTITION BY id
  ORDER BY
    TO_TIMESTAMP_NTZ(VALUE :created) DESC)) = 1;