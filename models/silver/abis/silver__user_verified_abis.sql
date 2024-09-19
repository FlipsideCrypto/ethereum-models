{{ config (
    materialized = "incremental",
    unique_key = "id",
    tags = ['abis']
) }}
{{ fsc_evm.silver_user_verified_abis () }}
{# WITH base AS (
SELECT
    contract_address,
    abi,
    PARSE_JSON(abi) AS DATA,
    SHA2(PARSE_JSON(abi)) AS abi_hash,
    discord_username,
    _inserted_timestamp
FROM
    {{ source(
        "crosschain_public",
        "user_abis"
    ) }}
WHERE
    blockchain = 'ethereum'
    AND NOT duplicate_abi

{% if is_incremental() %}
AND contract_address NOT IN (
    SELECT
        contract_address
    FROM
        {{ this }}
)
AND _inserted_timestamp > (
    SELECT
        COALESCE(
            MAX(
                _inserted_timestamp
            ),
            '1970-01-01'
        )
    FROM
        {{ this }}
)
AND _inserted_timestamp > DATEADD('day', -2, SYSDATE())
{% endif %}),
flat_event_abi AS (
    SELECT
        contract_address,
        _inserted_timestamp,
        DATA,
        VALUE :inputs AS inputs,
        VALUE :payable :: BOOLEAN AS payable,
        VALUE :stateMutability :: STRING AS stateMutability,
        VALUE :type :: STRING AS TYPE,
        VALUE :anonymous :: BOOLEAN AS anonymous,
        VALUE :name :: STRING AS NAME
    FROM
        base,
        LATERAL FLATTEN (
            input => DATA
        )
    WHERE
        TYPE = 'event' qualify ROW_NUMBER() over (
            PARTITION BY contract_address,
            NAME,
            inputs
            ORDER BY
                LENGTH(inputs)
        ) = 1
),
event_types AS (
    SELECT
        contract_address,
        _inserted_timestamp,
        inputs,
        anonymous,
        NAME,
        ARRAY_AGG(
            VALUE :type :: STRING
        ) AS event_type
    FROM
        flat_event_abi,
        LATERAL FLATTEN (
            input => inputs
        )
    GROUP BY
        contract_address,
        _inserted_timestamp,
        inputs,
        anonymous,
        NAME
),
apply_event_udfs AS (
    SELECT
        contract_address,
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
        utils.udf_evm_text_signature(abi) AS simple_event_name,
        utils.udf_keccak256(simple_event_name) AS event_signature,
        NAME,
        inputs,
        event_type,
        _inserted_timestamp
    FROM
        event_types
),
final_flat_event_abis AS (
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
        apply_event_udfs
),
flat_function_abis AS (
    SELECT
        contract_address,
        DATA,
        _inserted_timestamp,
        VALUE :inputs AS inputs,
        VALUE :outputs AS outputs,
        VALUE :payable :: BOOLEAN AS payable,
        VALUE :stateMutability :: STRING AS stateMutability,
        VALUE :type :: STRING AS TYPE,
        VALUE :name :: STRING AS NAME
    FROM
        base,
        LATERAL FLATTEN (
            input => DATA
        )
    WHERE
        TYPE = 'function'
),
udf_function_abis AS (
    SELECT
        *,
        PARSE_JSON(
            object_construct_keep_null(
                'inputs',
                IFNULL(
                    inputs,
                    []
                ),
                'outputs',
                IFNULL(
                    outputs,
                    []
                ),
                'name',
                NAME,
                'type',
                'function'
            ) :: STRING
        ) AS abi,
        utils.udf_evm_text_signature(abi) AS simple_function_name,
        utils.udf_keccak256(simple_function_name) AS function_signature
    FROM
        flat_function_abis qualify ROW_NUMBER() over (
            PARTITION BY contract_address,
            function_signature
            ORDER BY
                _inserted_timestamp DESC
        ) = 1
),
flat_inputs AS (
    SELECT
        contract_address,
        inputs,
        NAME,
        simple_function_name,
        function_signature,
        ARRAY_AGG(
            VALUE :type :: STRING
        ) AS inputs_type
    FROM
        udf_function_abis,
        LATERAL FLATTEN (
            input => inputs
        )
    GROUP BY
        ALL
),
fill_missing_input_names AS (
    SELECT
        contract_address,
        NAME,
        inputs_type,
        simple_function_name,
        function_signature,
        VALUE :internalType :: STRING AS internalType,
        VALUE :type :: STRING AS TYPE,
        CASE
            WHEN VALUE :name :: STRING = '' THEN CONCAT('input_', ROW_NUMBER() over (PARTITION BY contract_address, function_signature
            ORDER BY
                INDEX ASC) :: STRING)
                ELSE VALUE :name :: STRING
        END AS name_fixed,
        inputs,
        INDEX,
        VALUE :components AS components
    FROM
        flat_inputs,
        LATERAL FLATTEN (
            input => inputs
        )
),
final_flat_inputs AS (
    SELECT
        contract_address,
        NAME,
        inputs_type,
        simple_function_name,
        function_signature,
        ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'internalType',
                internalType,
                'name',
                name_fixed,
                'type',
                TYPE,
                'components',
                components
            )
        ) within GROUP (
            ORDER BY
                INDEX
        ) AS inputs
    FROM
        fill_missing_input_names
    GROUP BY
        ALL
),
flat_outputs AS (
    SELECT
        contract_address,
        outputs,
        simple_function_name,
        function_signature,
        NAME,
        ARRAY_AGG(
            VALUE :type :: STRING
        ) AS outputs_type
    FROM
        udf_function_abis,
        LATERAL FLATTEN (
            input => outputs
        )
    GROUP BY
        ALL
),
fill_missing_output_names AS (
    SELECT
        contract_address,
        NAME,
        outputs_type,
        simple_function_name,
        function_signature,
        VALUE :internalType :: STRING AS internalType,
        VALUE :type :: STRING AS TYPE,
        CASE
            WHEN VALUE :name :: STRING = '' THEN CONCAT('output_', ROW_NUMBER() over (PARTITION BY contract_address, function_signature
            ORDER BY
                INDEX ASC) :: STRING)
                ELSE VALUE :name :: STRING
        END AS name_fixed,
        outputs,
        INDEX,
        VALUE :components AS components
    FROM
        flat_outputs,
        LATERAL FLATTEN (
            input => outputs
        )
),
final_flat_outputs AS (
    SELECT
        contract_address,
        NAME,
        outputs_type,
        simple_function_name,
        function_signature,
        ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'internalType',
                internalType,
                'name',
                name_fixed,
                'type',
                TYPE,
                'components',
                components
            )
        ) within GROUP (
            ORDER BY
                INDEX
        ) AS outputs
    FROM
        fill_missing_output_names
    GROUP BY
        ALL
),
all_contracts AS (
    SELECT
        A.contract_address,
        A.name AS function_name,
        i.inputs,
        o.outputs,
        i.inputs_type,
        o.outputs_type,
        A._inserted_timestamp,
        A.function_signature,
        A.simple_function_name
    FROM
        udf_function_abis A
        LEFT JOIN final_flat_inputs i
        ON A.contract_address = i.contract_address
        AND A.function_signature = i.function_signature
        LEFT JOIN final_flat_outputs o
        ON A.contract_address = o.contract_address
        AND A.function_signature = o.function_signature
),
apply_function_udfs AS (
    SELECT
        contract_address,
        function_name,
        PARSE_JSON(
            object_construct_keep_null(
                'inputs',
                IFNULL(
                    inputs,
                    []
                ),
                'outputs',
                IFNULL(
                    outputs,
                    []
                ),
                'name',
                function_name,
                'type',
                'function'
            ) :: STRING
        ) AS abi,
        simple_function_name,
        function_signature,
        inputs,
        outputs,
        inputs_type,
        outputs_type,
        _inserted_timestamp
    FROM
        all_contracts
),
final_function_abis AS (
    SELECT
        contract_address,
        function_name,
        abi,
        simple_function_name,
        function_signature,
        inputs,
        outputs,
        inputs_type,
        outputs_type,
        _inserted_timestamp
    FROM
        apply_function_udfs
),
new_abis AS (
    SELECT
        DISTINCT contract_address
    FROM
        base
),
contracts AS (
    SELECT
        contract_address
    FROM
        {{ ref('silver__proxies') }}
        JOIN new_abis USING (contract_address)
),
proxies AS (
    SELECT
        p.proxy_address,
        p.contract_address
    FROM
        {{ ref('silver__proxies') }}
        p
        JOIN new_abis n
        ON p.proxy_address = n.contract_address
),
final_groupings AS (
    SELECT
        b.contract_address AS address,
        C.contract_address,
        proxy_address,
        CASE
            WHEN C.contract_address IS NOT NULL
            AND proxy_address IS NOT NULL THEN 'contract'
            WHEN C.contract_address IS NOT NULL THEN 'contract'
            WHEN proxy_address IS NOT NULL THEN 'proxy'
            WHEN C.contract_address IS NULL
            AND proxy_address IS NULL THEN 'contract'
        END AS TYPE,
        p.contract_address AS proxy_parent,
        CASE
            WHEN TYPE = 'contract' THEN address
            ELSE proxy_parent
        END AS final_address
    FROM
        base b
        LEFT JOIN (
            SELECT
                DISTINCT contract_address
            FROM
                contracts
        ) C
        ON b.contract_address = C.contract_address
        LEFT JOIN (
            SELECT
                DISTINCT proxy_address,
                contract_address
            FROM
                proxies
        ) p
        ON b.contract_address = proxy_address
),
identified_addresses AS (
    SELECT
        DISTINCT address AS base_address,
        final_address AS contract_address
    FROM
        final_groupings
),
function_mapping AS (
    SELECT
        ia.base_address,
        ia.contract_address,
        LEFT(
            function_signature,
            10
        ) AS function_sig
    FROM
        identified_addresses ia
        JOIN final_function_abis ffa
        ON ia.base_address = ffa.contract_address
),
valid_traces AS (
    SELECT
        DISTINCT base_address
    FROM
        (
            SELECT
                base_address
            FROM
                {{ ref('silver__traces') }}
                JOIN function_mapping
                ON function_sig = LEFT(
                    input,
                    10
                )
                AND IFF(
                    TYPE = 'DELEGATECALL',
                    from_address,
                    to_address
                ) = contract_address
            WHERE
                block_timestamp > DATEADD('month', -12, SYSDATE())
            LIMIT
                50000)
        ), event_mapping AS (
            SELECT
                ia.base_address,
                ia.contract_address,
                event_signature
            FROM
                identified_addresses ia
                JOIN final_flat_event_abis fea
                ON ia.base_address = fea.contract_address
        ),
        valid_logs AS (
            SELECT
                DISTINCT base_address
            FROM
                (
                    SELECT
                        base_address
                    FROM
                        {{ ref('silver__logs') }}
                        l
                        JOIN event_mapping ia
                        ON ia.contract_address = l.contract_address
                        AND event_signature = topics [0] :: STRING
                    WHERE
                        block_timestamp > DATEADD('month', -12, SYSDATE())
                    LIMIT
                        50000)
                ), all_valid_addresses AS (
                    SELECT
                        base_address
                    FROM
                        valid_traces
                    UNION
                    SELECT
                        base_address
                    FROM
                        valid_logs
                )
            SELECT
                contract_address,
                abi,
                discord_username,
                _inserted_timestamp,
                abi_hash,
                CONCAT(
                    contract_address,
                    '-',
                    abi_hash
                ) AS id
            FROM
                base
            WHERE
                contract_address IN (
                    SELECT
                        base_address
                    FROM
                        all_valid_addresses
                ) qualify(ROW_NUMBER() over(PARTITION BY contract_address
            ORDER BY
                _inserted_timestamp DESC)) = 1 #}
