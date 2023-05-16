{{ config (
    materialized = 'table'
) }}

WITH abi_base AS (

    SELECT
        contract_address,
        DATA,
        _inserted_timestamp
    FROM
        {{ ref('silver__abis') }}
),
flat_abi AS (
    SELECT
        contract_address,
        DATA,
        VALUE :inputs AS inputs,
        VALUE :payable :: BOOLEAN AS payable,
        VALUE :stateMutability :: STRING AS stateMutability,
        VALUE :type :: STRING AS TYPE,
        VALUE :anonymous :: BOOLEAN AS anonymous,
        VALUE :name :: STRING AS NAME,
        _inserted_timestamp
    FROM
        abi_base,
        LATERAL FLATTEN (
            input => DATA
        )
    WHERE
        TYPE = 'event'
),
event_types AS (
    SELECT
        contract_address,
        inputs,
        anonymous,
        NAME,
        _inserted_timestamp,
        ARRAY_AGG(
            VALUE :type :: STRING
        ) AS event_type
    FROM
        flat_abi,
        LATERAL FLATTEN (
            input => inputs
        )
    GROUP BY
        contract_address,
        inputs,
        anonymous,
        NAME,
        _inserted_timestamp
),
proxy_base AS (
    SELECT
        C.created_contract_address AS contract_address,
        p.proxy_address,
        p.start_block,
        C.block_number AS created_block
    FROM
        {{ ref('silver__created_contracts') }} C
        INNER JOIN {{ ref('silver__proxies2') }}
        p
        ON C.created_contract_address = p.contract_address
),
stacked AS (
    SELECT
        ea.contract_address,
        ea.inputs,
        ea.anonymous,
        ea.name,
        ea.event_type,
        pb.start_block,
        pb.contract_address AS base_contract_address
    FROM
        event_types ea
        INNER JOIN proxy_base pb
        ON ea.contract_address = pb.proxy_address
    UNION
    SELECT
        eab.contract_address,
        eab.inputs,
        eab.anonymous,
        eab.name,
        eab.event_type,
        pbb.created_block AS start_block,
        pbb.contract_address AS base_contract_address
    FROM
        event_types eab
        INNER JOIN (
            SELECT
                DISTINCT contract_address,
                created_block
            FROM
                proxy_base
        ) pbb
        ON eab.contract_address = pbb.contract_address
    UNION
    SELECT
        eac.contract_address,
        eac.inputs,
        eac.anonymous,
        eac.name,
        eac.event_type,
        0 AS start_block,
        eac.contract_address AS base_contract_address
    FROM
        event_types eac
    WHERE
        contract_address NOT IN (
            SELECT
                DISTINCT contract_address
            FROM
                proxy_base
        )
),
final_stacked AS (
    SELECT
        contract_address,
        inputs,
        anonymous,
        NAME,
        event_type,
        start_block,
        base_contract_address,
        REPLACE(
            event_type :: STRING,
            '"'
        ) AS event_type2,
        SUBSTR(event_type2, 2, LENGTH(event_type2) -2) AS event_type3
    FROM
        stacked),
        all_event_abis AS (
            SELECT
                contract_address AS source_contract_address,
                base_contract_address AS parent_contract_address,
                NAME AS event_name,
                PARSE_JSON(
                    OBJECT_CONSTRUCT(
                        'anonymous',
                        anonymous,
                        'inputs',
                        inputs,
                        'name',
                        NAME,
                        'type',
                        'event'
                    ) :: STRING
                ) AS abi,
                start_block,
                IFNULL(LEAD(start_block) over (PARTITION BY base_contract_address, NAME, event_type
            ORDER BY
                start_block) -1, 1e18) AS end_block,
                event_type3,
                CASE
                    WHEN event_type3 LIKE '%tuple[%' THEN TRUE
                    ELSE FALSE
                END AS array_f
            FROM
                final_stacked
        ),
        adjust_tuples AS (
            SELECT
                source_contract_address,
                parent_contract_address,
                event_name,
                event_type3,
                start_block,
                end_block,
                abi,
                VALUE :components AS components
            FROM
                all_event_abis,
                LATERAL FLATTEN (
                    input => abi :inputs
                )
            WHERE
                array_f
                AND VALUE :type :: STRING = 'tuple[]'
        ),
        flat_components AS (
            SELECT
                source_contract_address,
                parent_contract_address,
                event_name,
                event_type3,
                start_block,
                end_block,
                abi,
                VALUE :type :: STRING AS event_type,
                INDEX,
                seq
            FROM
                adjust_tuples,
                LATERAL FLATTEN (
                    input => components
                )
        ),
        agg_types AS (
            SELECT
                source_contract_address,
                parent_contract_address,
                event_name,
                event_type3,
                start_block,
                end_block,
                abi,
                seq,
                ARRAY_AGG(event_type) AS event_type_add
            FROM
                flat_components
            GROUP BY
                source_contract_address,
                parent_contract_address,
                event_name,
                event_type3,
                start_block,
                end_block,
                abi,
                seq
        ),
        adj_names AS (
            SELECT
                *,
                REPLACE(
                    event_type_add :: STRING,
                    '"'
                ) AS event_type_add2,
                CONCAT(
                    '(',
                    SUBSTR(event_type_add2, 2, LENGTH(event_type_add2) -2),
                    ')[]') AS event_type_add3
                    FROM
                        agg_types
                ),
                agg_names AS (
                    SELECT
                        source_contract_address,
                        parent_contract_address,
                        event_name,
                        event_type3,
                        start_block,
                        end_block,
                        abi,
                        LISTAGG(
                            event_type_add3,
                            ','
                        ) AS abi_adjustment
                    FROM
                        adj_names
                    GROUP BY
                        source_contract_address,
                        parent_contract_address,
                        event_name,
                        event_type3,
                        start_block,
                        end_block,
                        abi
                ),
                final_adj_tuple_abis AS (
                    SELECT
                        source_contract_address,
                        parent_contract_address,
                        event_name,
                        event_type3,
                        start_block,
                        end_block,
                        abi,
                        abi_adjustment,
                        CONCAT(event_name, '(', REPLACE(event_type3, ',tuple[]'), ',', abi_adjustment, ')') AS final_adjusted_abi
                    FROM
                        agg_names
                )
            SELECT
                source_contract_address,
                parent_contract_address,
                event_name,
                abi,
                start_block,
                end_block,
                silver.udf_encode_events(
                    CONCAT(
                        event_name,
                        '(',
                        event_type3,
                        ')'
                    )
                ) AS event_sig,
                event_type3,
                event_type3 AS simplified_abi
            FROM
                all_event_abis
            WHERE
                NOT array_f
            UNION ALL
            SELECT
                source_contract_address,
                parent_contract_address,
                event_name,
                abi,
                start_block,
                end_block,
                silver.udf_encode_events(
                    final_adjusted_abi
                ) AS event_sig,
                event_type3,
                final_adjusted_abi AS simplified_abi
            FROM
                final_adj_tuple_abis
