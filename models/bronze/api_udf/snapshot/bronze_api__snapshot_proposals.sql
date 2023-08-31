{{ config(
    materialized = 'incremental',
    unique_key = 'proposal_id',
    full_refresh = false,
    tags = ['snapshot']
) }}

WITH max_time AS (

{% if is_incremental() %}

SELECT
    MAX(created_at) AS max_prop_created
FROM
    {{ this }}
{% else %}
SELECT
    0 AS max_prop_created
{% endif %}),
ready_prop_requests AS (
    SELECT
        CONCAT(
            'query { proposals(orderBy: "created", orderDirection: asc,first:1000,where:{created_gte: ',
            max_time_created,
            '}) { id space{id} ipfs author created network type title body start end state votes choices scores_state scores } }'
        ) AS proposal_request_created
    FROM
        (
            SELECT
                DATE_PART(
                    epoch_second,
                    max_prop_created :: TIMESTAMP
                ) AS max_time_created
            FROM
                max_time
        )
),
proposal_data_created AS (
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
            ) },{ 'query': proposal_request_created }
        ) AS resp,
        SYSDATE() AS _inserted_timestamp
    FROM
        ready_prop_requests
),
proposals_final AS (
    SELECT
        VALUE :id :: STRING AS proposal_id,
        VALUE :ipfs :: STRING AS ipfs,
        STRTOK_TO_ARRAY(
            VALUE :choices,
            ';'
        ) AS choices,
        VALUE :author :: STRING AS proposal_author,
        VALUE :title :: STRING AS proposal_title,
        VALUE :body :: STRING AS proposal_text,
        VALUE :space :id :: STRING AS space_id,
        VALUE :network :: STRING AS network,
        TO_TIMESTAMP_NTZ(
            VALUE :created
        ) AS created_at,
        TO_TIMESTAMP_NTZ(
            VALUE :start
        ) AS proposal_start_time,
        TO_TIMESTAMP_NTZ(
            VALUE :end
        ) AS proposal_end_time,
        VALUE,
        _inserted_timestamp
    FROM
        proposal_data_created,
        LATERAL FLATTEN(
            input => resp :data :data :proposals
        )
    WHERE
        choices IS NOT NULL
        AND proposal_author IS NOT NULL
        AND proposal_title IS NOT NULL
        AND proposal_text IS NOT NULL
        AND proposal_start_time IS NOT NULL
        AND proposal_end_time IS NOT NULL
        AND space_id IS NOT NULL
        AND network IS NOT NULL qualify(ROW_NUMBER() over (PARTITION BY proposal_id
    ORDER BY
        TO_TIMESTAMP_NTZ(VALUE :created) DESC)) = 1
)
SELECT
    proposal_id,
    ipfs,
    choices,
    proposal_author,
    proposal_title,
    proposal_text,
    space_id,
    network,
    created_at,
    proposal_start_time,
    proposal_end_time,
    _inserted_timestamp
FROM
    proposals_final
