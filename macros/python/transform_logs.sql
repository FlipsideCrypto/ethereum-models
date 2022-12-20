{% macro create_udf_transform_logs(schema) %}
create or replace function {{ schema }}.udf_transform_logs(decoded variant)
returns variant 
language python 
runtime_version = '3.8' 
handler = 'transform' as $$
from copy import deepcopy

def transform_event(event: dict):
    new_event = deepcopy(event)
    if new_event.get("components"):
        components = new_event.get("components")
        for iy, y in enumerate(new_event["value"]):
            for i, c in enumerate(components):
                y[i] = {"value": y[i], **c}
            new_event["value"][iy] = {z["name"]: z["value"] for z in y}
        return new_event
    else:
        return event


def transform(events: list):
    try:
        results = [
            transform_event(event) if event["decoded"] else event
            for event in events["data"]
        ]
        events["data"] = results
        return events
    except:
        return events
$$;

{% endmacro %}