DELETE FROM {{ params.destination.schema }}.{{ params.destination.table }}
USING   temp_{{ params.destination.table }}_{{ ts_nodash }} AS n
WHERE   {% for column in params.columns.id -%}
        {% if loop.index > 1 %} AND {% endif %}"{{ params.destination.table }}".{{ column }} = n.{{ column }}
        {% endfor %}
    {% if params.columns.audit %}
    AND GREATEST({% for column in params.columns.audit -%}{% if loop.index > 1 %}, {% endif %}"{{ params.destination.table }}".{{ column }}{% endfor %})
        <= GREATEST({% for column in params.columns.audit -%}{% if loop.index > 1 %}, {% endif %}n.{{ column }}{% endfor %})
    {% endif %}