{% macro create_udf_transform_logs(schema) %}
create or replace function {{ schema }}.udf_transform_logs(decoded array)
returns array 
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
                if isinstance(y, list):
                    x["value"][iy] = {z["name"]: z["value"] for z in y for y in x["value"]}
            results.append(x)
        else:
            results.append(x)
    return results

def transform(events: list):
    return [transform_event(event) if event["decoded"] else event for event in events]

$$;

{% endmacro %}