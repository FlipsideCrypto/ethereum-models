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
  select hex_to_int('ffffffffffffffffffffffffffffffffffffffffffffffffffffffffe5b83acf');
  >> -440911153
  """
  if (len(hex) == 64 and hex[0] == 'f') or (len(hex) == 66 and hex[2] == 'f'):
    while(hex[0] == 'f'):
      hex = hex[1:]
    int_val = int(hex, 16)
    if int_val & 1 << 32 - 1 != 0:
      int_val = int_val - (1 << 32)
      return int_val

  return (str(int(hex, 16)) if hex else None)
$$;
{% endmacro %}
