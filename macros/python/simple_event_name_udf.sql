{% macro create_udf_simple_event_names(schema) %}
create or replace function {{ schema }}.udf_simple_event_name(abi VARIANT)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'get_simplified_signature'
AS
$$
def get_simplified_signature(abi):
    def generate_signature(inputs):
        signature_parts = []
        for input_data in inputs:
            if 'components' in input_data:
                component_signature_parts = []
                components = input_data['components']
                component_signature_parts.extend(generate_signature(components))
                component_signature_parts[-1] = component_signature_parts[-1].rstrip(",")
                if input_data['type'].endswith('[]'):
                    signature_parts.append("(" + "".join(component_signature_parts) + ")[],")
                else:
                    signature_parts.append("(" + "".join(component_signature_parts) + "),")
            else:
                signature_parts.append(input_data['type'].replace('enum ', '').replace(' payable', '') + ",")
        return signature_parts

    signature_parts = [abi['name'] + "("]
    signature_parts.extend(generate_signature(abi['inputs']))
    signature_parts[-1] = signature_parts[-1].rstrip(",") + ")"
    return "".join(signature_parts)
$$;
{% endmacro %}