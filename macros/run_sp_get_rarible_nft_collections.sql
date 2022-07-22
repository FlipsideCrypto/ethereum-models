{% macro run_sp_get_rarible_nft_collections() %}
    {% set query %}
        call streamline.sp_get_rarible_nft_collections();
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}