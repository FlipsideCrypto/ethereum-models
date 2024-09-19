-- depends_on: {{ ref('bronze__streamline_contract_abis') }}
{{ config (
    materialized = "incremental",
    unique_key = "contract_address",
    merge_update_columns = ["contract_address"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(contract_address)",
    tags = ['abis']
) }}
{{ fsc_evm.silver_verified_abis () }}
