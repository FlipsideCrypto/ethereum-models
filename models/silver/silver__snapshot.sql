{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy = 'delete+insert',
    tags = ['core']
) }}

WITH proposals AS (
  SELECT 
      i.value :id :: STRING AS proposal_id, 
      i.value :ipfs :: STRING AS ipfs, 
      STRTOK_TO_ARRAY(i.value :choices, ';') AS choices, 
      i.value :author :: STRING AS proposal_author, 
      i.value :title :: STRING AS proposal_title, 
      i.value :body :: STRING AS proposal_text, 
      i.value :space_id :: STRING AS space_id, 
      TO_TIMESTAMP_NTZ(i.value :start) AS proposal_start_time, 
      TO_TIMESTAMP_NTZ(i.value :end) AS proposal_end_time
  FROM 
    {{ source( 
        'bronze',
        'bronze_snapshot_719356055'
    ) }}, 
  LATERAL FLATTEN (input => record_content) i
  WHERE 
    record_metadata:key LIKE '%govy-props%'
    AND proposal_author IS NOT NULL
    AND proposal_text IS NOT NULL
    AND proposal_end_time IS NOT NULL
    AND proposal_start_time IS NOT NULL 
    AND space_id IS NOT NULL
    AND proposal_title IS NOT NULL 
    AND choices IS NOT NULL

qualify(ROW_NUMBER() over(PARTITION BY proposal_id
  ORDER BY
    TO_TIMESTAMP_NTZ(i.value :created) DESC)) = 1
),  

votes AS ( 
    SELECT 
        SPLIT(i.value :choice :: STRING, ';') AS vote_option,
        i.value :id :: STRING AS id,
        i.value :ipfs :: STRING AS ipfs, 
        i.value :prop_id :: STRING AS proposal_id, 
        i.value :voter :: STRING AS voter, 
        TO_TIMESTAMP_NTZ(i.value :created) AS _inserted_timestamp
    FROM {{ source( 
        'bronze',
        'bronze_snapshot_719356055'
    ) }}, 
    LATERAL FLATTEN (input => record_content) i 
    WHERE record_metadata:key LIKE '%govy-votes%'

{% if is_incremental() %}
    AND TO_TIMESTAMP_NTZ(i.value :created) >= CURRENT_DATE -2 
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY id
  ORDER BY
    TO_TIMESTAMP_NTZ(i.value :created) DESC)) = 1
)

SELECT 
    id, 
    v.proposal_id, 
    voter, 
    vote_option, 
    choices, 
    proposal_author, 
    proposal_title, 
    proposal_text, 
    space_id, 
    proposal_start_time, 
    proposal_end_time, 
    _inserted_timestamp
 FROM votes v
 
 LEFT OUTER JOIN proposals p
 ON v.proposal_id = p.proposal_id

