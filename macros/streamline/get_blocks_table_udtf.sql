{% macro create_udtf_get_blocks_table(schema) %}
create or replace function {{ schema }}.udtf_get_blocks_table(max_height integer)
returns table (height number)
as
$$
    with blocks as (
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
    blocks
where
    id <= max_height
$$
;

{% endmacro %}