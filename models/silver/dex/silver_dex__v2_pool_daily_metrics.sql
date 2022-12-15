{{ config(
    materialized = 'incremental',
    persist_docs ={ "relation": true,
    "columns": true },
    unique_key = 'id',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH reads AS (

    SELECT
        CASE
            WHEN function_signature = '0x1526fe27' THEN CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40))
            ELSE contract_address
        END AS contract_address,
        block_number,
        function_signature,
        segmented_data,
        _inserted_timestamp
    FROM
        {{ ref('bronze__successful_reads') }}
    WHERE
        call_name IN (
            'Balance_of_SLP_staked',
            'Total_SLP_issued'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE
    FROM
        {{ this }}
)
{% endif %}
),
block_date AS (
    SELECT
        block_timestamp :: DATE AS DATE,
        block_number,
        _inserted_timestamp
    FROM
        {{ ref('silver__blocks') }}
    WHERE
        DATE >= '2020-02-01'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE
    FROM
        {{ this }}
)
{% endif %}
),
contracts AS (
    SELECT
        DISTINCT block_number,
        contract_address
    FROM
        reads
),
balance_of_slp_staked AS (
    SELECT
        block_number,
        contract_address,
        PUBLIC.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) AS balance_of_slp_staked
    FROM
        reads
    WHERE
        function_signature = '0x70a08231'
),
total_supply_of_SLP AS (
    SELECT
        block_number,
        contract_address,
        PUBLIC.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) AS total_supply_of_SLP
    FROM
        reads
    WHERE
        function_signature = '0x18160ddd'
),
base AS (
    SELECT
        A.date,
        A.block_number,
        b.contract_address,
        A._inserted_timestamp
    FROM
        block_date A
        RIGHT JOIN contracts b
        ON A.block_number = b.block_number
),
FINAL AS (
    SELECT
        A.date,
        A.block_number,
        A.contract_address,
        d.balance_of_slp_staked,
        e.total_supply_of_SLP,
        A._inserted_timestamp AS _inserted_timestamp,
        {{ dbt_utils.surrogate_key(
            ['a.block_number', 'a.contract_address', 'a.date']
        ) }} AS id
    FROM
        base A
        LEFT JOIN balance_of_slp_staked d
        ON A.block_number = d.block_number
        AND A.contract_address = d.contract_address
        LEFT JOIN total_supply_of_SLP e
        ON A.block_number = e.block_number
        AND A.contract_address = e.contract_address
)
SELECT
    *
FROM
    FINAL
WHERE
    block_number IS NOT NULL
