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
  return (str(int(hex, 16)) if hex and hex != "0x" else None)
$$;

{% endmacro %}


{% macro create_udf_hex_to_int_with_inputs(schema) %}
create or replace function {{ schema }}.udf_hex_to_int(encoding string, hex string)
returns string
language python
runtime_version = '3.8'
handler = 'hex_to_int'
as
$$
def hex_to_int(encoding, hex) -> str:
  """
  Converts hex (of any size) to int (as a string). Snowflake and java script can only handle up to 64-bit (38 digits of precision)
  select hex_to_int('hex', '200000000000000000000000000000211');
  >> 680564733841876926926749214863536423441
  select hex_to_int('hex', '0x200000000000000000000000000000211');
  >> 680564733841876926926749214863536423441
  select hex_to_int('hex', NULL);
  >> NULL
  select hex_to_int('s2c', 'ffffffffffffffffffffffffffffffffffffffffffffffffffffffffe5b83acf');
   >> -440911153
  """
  if not hex:
    return None
  if encoding.lower() == 's2c':
    if hex[0:2].lower() != '0x':
      hex = f'0x{hex}'

    bits = len(hex[2:])*4
    value = int(hex, 0)
    if value & (1 << (bits-1)):
        value -= 1 << bits
    return str(value)
  else:
    return str(int(hex, 16))

$$;

{% endmacro %}