{% macro create_udf_hex_to_int(schema) %}
create or replace function {{ schema }}.udf_hex_to_int(hex string)
returns string
language python
runtime_version = '3.8'
handler = 'hex_to_int'
as
$$
def hex_to_int(hex) -> str:
  """
  Converts hex (of any size) to int (as a string). Snowflake and java script can only handle up to 64-bit (38 digits of precision)

  select hex_to_int('200000000000000000000000000000211');
  >> 680564733841876926926749214863536423441
  select hex_to_int('0x200000000000000000000000000000211');
  >> 680564733841876926926749214863536423441
  select hex_to_int(NULL);
  >> NULL
  """
  return str(int(hex, 16)) if hex else None
$$;
{% endmacro %}
