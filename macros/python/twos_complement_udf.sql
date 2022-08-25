{% macro create_udf_twos_complement(schema) %}
create or replace function {{ schema }}.udf_twos_complement(twosc string)
returns string
language python
runtime_version = '3.8'
handler = 'twos_complement'
as
$$
import ctypes
def twos_complement(twosc) -> str:
  """
  Converts hex (of any size) to int (as a string). Snowflake and java script can only handle up to 64-bit (38 digits of precision)

  select twos_complement(NULL);
  >> NULL
  select twos_complement('ffffffffffffffffffffffffffffffffffffffffffffffffffffffffe5b83acf');
  >> -440911153
  """
  return (str(ctypes.c_int32(int(twosc, 16)).value) if hex else None)
$$;

{% endmacro %}
