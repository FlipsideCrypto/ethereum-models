{{ config(
    materialized = 'incremental',
    unique_key = 'proposal_id',
    full_refresh = true
) }}

WITH max_time AS (
    {% if is_incremental() %}
    SELECT
        MAX(proposal_start_time) AS max_prop_start,
        MAX(proposal_end_time) AS max_prop_end
    FROM
        {{ this }}
    {% else %}
    SELECT
        0 AS max_prop_start,
        0 AS max_prop_end
    {% endif %}
),

ready_prop_requests AS (
    SELECT
        CONCAT(
            'query { proposals(orderBy: "created", orderDirection: asc,first:2000,where:{created_gt: ',
            max_time_start,
            '}) { id space{id} ipfs author created network type title body start end state votes choices scores_state scores } }'
        ) AS proposal_request_created,
        CONCAT(
            'query { proposals(orderBy: "created", orderDirection: asc,first:2000,where:{end_gt: ',
            max_time_end,
            '}) { id space{id} ipfs author created network type title body start end state votes choices scores_state scores } }'
        ) AS proposal_request_end
    FROM (
        SELECT 
            DATE_PART(epoch_second, max_prop_start::TIMESTAMP) AS max_time_start,
            DATE_PART(epoch_second, max_prop_end::TIMESTAMP) AS max_time_end 
        FROM 
            max_time
    )
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
    VALUE,
    _inserted_timestamp
FROM
    proposal_data_created,
LATERAL FLATTEN(
    input => resp :data :data :proposals
)
QUALIFY(ROW_NUMBER() over(PARTITION BY proposal_id
  ORDER BY
    TO_TIMESTAMP_NTZ(VALUE :created) DESC)) = 1
UNION --confirm that calling on start and end is not duplicative
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
    VALUE,
    _inserted_timestamp
FROM
    proposal_data_end,
LATERAL FLATTEN(
    input => resp :data :data :proposals
)
QUALIFY(ROW_NUMBER() over(PARTITION BY proposal_id
  ORDER BY
    TO_TIMESTAMP_NTZ(VALUE :end) DESC)) = 1
)

--confirm use case for filtering via above WHERE clause (taken from original silver model)

SELECT
    proposal_id,
    ipfs,
    choices,
    proposal_author,
    proposal_title,
    proposal_text,
    space_id,
    proposal_start_time,
    proposal_end_time,
    _inserted_timestamp
FROM proposals_final