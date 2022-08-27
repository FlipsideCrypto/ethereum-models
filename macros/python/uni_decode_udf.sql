{% macro create_uni_decode(schema) %}
create or replace function {{ schema }}.uni_decode(hex string)
returns string
language python
runtime_version = '3.8'
handler = 'uni_decode'
as
$$
def uni_decode(hexstr):

  """
hex = '0xffffffffffffffffffffffffffffffffffffffffffffffff76a042212b2e29e4'
decimal = hex_to_dec(hex)

print(decimal)

>> -9898839270734550556
  """
    bits = len(hexstr[2:])*4
    value = int(hexstr,0)

    if value & (1 << (bits-1)):
        value -= 1 << bits
    return value
$$;
{% endmacro %}
