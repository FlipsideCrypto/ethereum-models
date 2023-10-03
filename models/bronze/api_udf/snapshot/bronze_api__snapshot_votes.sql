{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    full_refresh = false,
    tags = ['snapshot'],
    enabled = false
) }}

WITH initial_votes_request AS (
{% for item in range(6) %}
(
    SELECT
        ethereum.streamline.udf_api(
            'GET',
            'https://hub.snapshot.org/graphql',
            {
                'apiKey': (
                    SELECT
                        api_key
                    FROM
                        {{ source(
                            'crosschain_silver',
                             'apis_keys'
                             ) }}
                    WHERE
                        api_name = 'snapshot'
                )
            },
            {
                'query': 'query { votes(orderBy: "created", orderDirection: asc, first: 1000, skip: ' || {{ item * 1000 }} || ', where:{created_gte: ' || max_time_start || ',created_lt: ' || max_time_end || '}) { id proposal{id} ipfs voter created choice vp } }'
            }
        ) AS resp,
        SYSDATE() AS _inserted_timestamp
    FROM (
        SELECT
            DATE_PART(epoch_second, max_vote_start :: TIMESTAMP) AS max_time_start,
            DATE_PART(epoch_second, max_vote_start :: TIMESTAMP) + 86400 AS max_time_end
        FROM (
            {% if is_incremental() %}
            SELECT
                MAX(vote_timestamp) AS max_vote_start
            FROM
                {{ this }}
            {% else %}
            SELECT
                0 AS max_vote_start
            {% endif %}
        ) AS max_time
    )
)
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
),

votes_initial AS (
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
        initial_votes_request,
        LATERAL FLATTEN(
            input => resp :data :data :votes
        ) qualify(ROW_NUMBER() over(PARTITION BY id
    ORDER BY
        TO_TIMESTAMP_NTZ(VALUE :created) DESC)) = 1
),

final_votes_request AS (
{% for item in range(6) %}
(
    SELECT
        ethereum.streamline.udf_api(
            'GET',
            'https://hub.snapshot.org/graphql',
            {
                'apiKey': (
                    SELECT
                        api_key
                    FROM
                        {{ source(
                            'crosschain_silver',
                             'apis_keys'
                             ) }}
                    WHERE
                        api_name = 'snapshot'
                )
            },
            {
                'query': 'query { votes(orderBy: "created", orderDirection: asc, first: 1000, skip: ' || {{ item * 1000 }} || ', where:{created_gte: ' || max_time_start || ',created_lt: ' || max_time_end || '}) { id proposal{id} ipfs voter created choice vp } }'
            }
        ) AS resp,
        SYSDATE() AS _inserted_timestamp
    FROM (
        SELECT
            DATE_PART(epoch_second, max_vote_start :: TIMESTAMP) AS max_time_start,
            DATE_PART(epoch_second, max_vote_start :: TIMESTAMP) + 86400 AS max_time_end
        FROM (
            SELECT
                MAX(vote_timestamp) AS max_vote_start
            FROM
                votes_initial
        ) AS max_time
    )
)
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
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
        final_votes_request,
        LATERAL FLATTEN(
            input => resp :data :data :votes
        ) 
    qualify(ROW_NUMBER() over(PARTITION BY id
    ORDER BY
        TO_TIMESTAMP_NTZ(VALUE :created) DESC)) = 1
),

votes_merged AS (
    SELECT
        *
    FROM
        votes_initial
    UNION ALL
    SELECT
        *
    FROM
        votes_final
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
    votes_merged
    INNER JOIN {{ ref('bronze_api__snapshot_proposals') }} USING (proposal_id)
qualify(ROW_NUMBER() over(PARTITION BY id
    ORDER BY
        _inserted_timestamp DESC)) = 1