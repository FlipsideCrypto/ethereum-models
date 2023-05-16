{% macro create_udf_keccak(schema) %}
create or replace function {{ schema }}.udf_keccak(event_name VARCHAR(255))
RETURNS string
LANGUAGE python
runtime_version = 3.8
packages = ('pycryptodome==3.15.0')
HANDLER = 'udf_encode'
AS
$$
from Crypto.Hash import keccak

def udf_encode(event_name):
    keccak_hash = keccak.new(digest_bits=256)
    keccak_hash.update(event_name.encode('utf-8'))
    return '0x' + keccak_hash.hexdigest()
$$;

{% endmacro %}