{% macro create_udtf_get_base_table(schema) %}
create or replace function {{ schema }}.udtf_get_base_table(max_height integer)
returns table (height number)
as
$$
    with base as (
        select
            row_number() over (
                order by
                    seq4()
            ) as id
        from
            table(generator(rowcount => 100000000))
    )
select
    id as height
from
    base
where
    id <= max_height
$$
;

{% endmacro %}