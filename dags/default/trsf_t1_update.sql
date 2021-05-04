UPDATE {{ params.destination.schema }}.{{ params.destination.table }} AS trsf
SET
    {% for column in params.columns.all -%}
    {{ column.name }} = stg.{{ column.name }},
    {% endfor %}
    dwh_updated_at    = CURRENT_TIMESTAMP
FROM stg_{{ params.destination.table }} AS stg
WHERE   {% for column in params.columns.bk -%}
            {% if loop.index > 1 %}AND {% endif %} stg.{{ column }} = trsf.{{ column }}
        {% endfor %}
   AND ({% for column in params.columns.all -%}
            {% if loop.index > 1 %} OR {% endif %}
            ((stg.{{ column.name }} IS NULL AND trsf.{{ column.name }} IS NOT NULL) OR (stg.{{ column.name }} IS NOT NULL AND trsf.{{ column.name }} IS NULL) OR (stg.{{ column.name }} != trsf.{{ column.name }}))
        {% endfor %})
