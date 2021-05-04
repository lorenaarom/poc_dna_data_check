DELETE FROM {{ params.destination.schema }}.{{ params.destination.table }}
USING   stg_{{ params.destination.table }} AS n
WHERE   {% for column in params.columns.bk -%}
        {% if loop.index > 1 %} AND {% endif %}"{{ params.destination.table }}".{{ column }} = n.{{ column }}
        {% endfor %}
