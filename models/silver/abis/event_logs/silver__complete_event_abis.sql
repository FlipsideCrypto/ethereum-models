{{ config (
    materialized = 'incremental',
    unique_key = ['parent_contract_address','event_signature','start_block'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    tags = ['abis']
) }}

WITH proxies AS (

    SELECT
        created_block,
        proxy_created_block,
        contract_address,
        proxy_address,
        start_block,
        _id,
        _inserted_timestamp
    FROM
        {{ ref('silver__proxies') }}
),
flat_abis AS (
    SELECT
        contract_address,
        event_name,
        abi,
        simple_event_name,
        event_signature,
        NAME,
        inputs,
        event_type,
        _inserted_timestamp
    FROM
        {{ ref('silver__flat_event_abis') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '24 hours'
        FROM
            {{ this }}
    )
    OR contract_address IN (
        SELECT
            DISTINCT contract_address AS contract_address
        FROM
            proxies
        UNION ALL
        SELECT
            DISTINCT proxy_address AS contract_address
        FROM
            proxies
    )
{% endif %}
),
base AS (
    SELECT
        ea.contract_address,
        event_name,
        abi,
        simple_event_name,
        event_signature,
        NAME,
        inputs,
        event_type,
        ea._inserted_timestamp,
        pb._inserted_timestamp AS proxy_inserted_timestamp,
        pb.start_block,
        pb.proxy_created_block,
        pb.contract_address AS base_contract_address,
        1 AS priority
    FROM
        flat_abis ea
        JOIN proxies pb
        ON ea.contract_address = pb.proxy_address
    UNION ALL
    SELECT
        eab.contract_address,
        event_name,
        abi,
        simple_event_name,
        event_signature,
        NAME,
        inputs,
        event_type,
        eab._inserted_timestamp,
        pbb._inserted_timestamp AS proxy_inserted_timestamp,
        pbb.created_block AS start_block,
        pbb.proxy_created_block,
        pbb.contract_address AS base_contract_address,
        2 AS priority
    FROM
        flat_abis eab
        JOIN (
            SELECT
                DISTINCT contract_address,
                created_block,
                proxy_created_block,
                _inserted_timestamp
            FROM
                proxies
        ) pbb
        ON eab.contract_address = pbb.contract_address
    UNION ALL
    SELECT
        contract_address,
        event_name,
        abi,
        simple_event_name,
        event_signature,
        NAME,
        inputs,
        event_type,
        _inserted_timestamp,
        NULL AS proxy_inserted_timestamp,
        0 AS start_block,
        NULL AS proxy_created_block,
        contract_address AS base_contract_address,
        3 AS priority
    FROM
        flat_abis eac
    WHERE
        contract_address NOT IN (
            SELECT
                DISTINCT contract_address
            FROM
                proxies
        )
),
new_records AS (
    SELECT
        base_contract_address AS parent_contract_address,
        event_name,
        abi,
        start_block,
        proxy_created_block,
        simple_event_name,
        event_signature,
        NAME,
        inputs,
        event_type,
        _inserted_timestamp,
        proxy_inserted_timestamp
    FROM
        base qualify ROW_NUMBER() over (
            PARTITION BY parent_contract_address,
            NAME,
            event_type,
            event_signature,
            start_block
            ORDER BY
                priority ASC,
                _inserted_timestamp DESC,
                proxy_created_block DESC NULLS LAST,
                proxy_inserted_timestamp DESC NULLS LAST
        ) = 1
)
SELECT
    parent_contract_address,
    event_name,
    abi,
    start_block,
    proxy_created_block,
    simple_event_name,
    event_signature,
    IFNULL(LEAD(start_block) over (PARTITION BY parent_contract_address, event_signature
ORDER BY
    start_block) -1, 1e18) AS end_block,
    _inserted_timestamp,
    proxy_inserted_timestamp,
    SYSDATE() AS _updated_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['parent_contract_address','event_signature','start_block']
    ) }} AS complete_event_abis_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    new_records qualify ROW_NUMBER() over (
        PARTITION BY parent_contract_address,
        event_name,
        event_signature,
        start_block
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
