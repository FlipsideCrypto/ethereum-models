{% macro create_udf_transform_logs(schema) %}
create or replace function {{ schema }}.udf_transform_logs(decoded variant)
returns variant 
language python 
runtime_version = '3.8' 
handler = 'transform' as $$
from copy import deepcopy

def transform_event(event: dict):
    event_ = deepcopy(event)
    if event_.get("components"):
        components = event_.get("components")
        results = []
        for iy, y in enumerate(event_["value"]):
            for i, c in enumerate(components):
                y[i] = {"value": y[i], **c}
            event["value"][iy] = {z["name"]: z["value"] for z in y}
        results.append(event)
        return results
    else:
        return event_


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