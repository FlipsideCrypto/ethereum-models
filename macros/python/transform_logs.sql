{% macro create_udf_transform_logs(schema) %}
create or replace function {{ schema }}.udf_transform_logs(decoded variant)
returns variant 
language python 
runtime_version = '3.8' 
handler = 'transform' as $$
def transform_event(event: dict):
    results = []
    for x in event["data"]:
        if x.get("components"):
            components = x.get("components")
            for iy, y in enumerate(x["value"]):
                for i, c in enumerate(components):
                    y[i] = {"value": y[i], **components[i]}
                if isinstance(y, list) and isinstance(x["value"], list):
                    x["value"][iy] = {z["name"]: z["value"] for z in y}
            results.append(x)
        else:
            results.append(x)
    return results

def transform(events: list):
    try:
        return [transform_event(event) if event["decoded"] else event for event in events]
    except:
        return events
$$;

{% endmacro %}