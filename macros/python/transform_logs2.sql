{% macro create_udf_transform_logs2(schema) %}
create or replace function {{ schema }}.udf_transform_logs2(decoded variant)
returns variant 
language python 
runtime_version = '3.8' 
handler = 'transform' as $$
from copy import deepcopy

def transform_tuple(components: list, values: list):
    mapped_values = {}
    for i, component in enumerate(components):
        if i < len(values):
            if component["type"] == "tuple[]":
                mapped_values[component["name"]] = [transform_tuple(component["components"], v) for v in values[i]]
            else:
                mapped_values[component["name"]] = values[i]
    return mapped_values

def transform_event(event: dict):
    new_event = deepcopy(event)
    if new_event.get("components"):
        components = new_event.get("components")
        transformed_data = {}
        
        if isinstance(new_event["value"][0], list):
            result_list = []
            for value_set in new_event["value"]:
                result_list.append(transform_tuple(components, value_set))
            transformed_data["value"] = result_list

        else:
            transformed_data["value"] = transform_tuple(components, new_event["value"])

        new_event.update(transformed_data)
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