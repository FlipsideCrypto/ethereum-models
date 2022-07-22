{% macro run_sp_get_rarible_nft_metadata() %}
    {% set query %}
        call streamline.sp_get_rarible_nft_metadata();
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}