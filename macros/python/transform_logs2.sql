{% macro create_udf_transform_logs2(schema) %}
create or replace function {{ schema }}.udf_transform_logs2(decoded variant)
returns variant 
language python 
runtime_version = '3.8' 
handler = 'transform' as $$
from copy import deepcopy

def transform_tuple(components: list, values: list):
    transformed_values = []
    for i, component in enumerate(components):
        if i < len(values):
            if component["type"] == "tuple[]":
                sub_values = [transform_tuple(component["components"], v) for v in values[i]]
                transformed_values.append({"value": sub_values, **component})
            else:
                transformed_values.append({"value": values[i], **component})
    return {item["name"]: item["value"] for item in transformed_values}

def transform_event(event: dict):
    new_event = deepcopy(event)
    if new_event.get("components"):
        components = new_event.get("components")
        
        if isinstance(new_event["value"][0], list):
            result_list = []
            for value_set in new_event["value"]:
                result_list.append(transform_tuple(components, value_set))
            new_event["value"] = result_list

        else:
            new_event["value"] = transform_tuple(components, new_event["value"])

        return new_event

    else:
        return event

def transform(events: dict):
    try:
        results = [
            transform_event(event) if event.get("decoded") else event
            for event in events["data"]
        ]
        events["data"] = results
        return events
    except:
        return events
$$;

{% endmacro %}