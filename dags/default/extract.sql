SELECT  {% for column in params.columns.all -%}
        {% if loop.index > 1 %}, {% endif %}{{ column.name }}
        {% endfor %}
        {% if params.columns.delete %}, {{ params.columns.delete }}{% endif %}
FROM    {{ params.origin.schema }}.{{ params.origin.table }}
{% if params.columns.audit %}
WHERE   GREATEST({% for column in params.columns.audit -%}{% if loop.index > 1 %}, {% endif -%}{{ column }}{% endfor %}) <  to_timestamp('{{ ts_nodash }}', 'YYYYMMDD"T"HH24MISS')
    AND GREATEST({% for column in params.columns.audit -%}{% if loop.index > 1 %}, {% endif -%}{{ column }}{% endfor %}) >= to_timestamp('{{ ts_nodash }}', 'YYYYMMDD"T"HH24MISS') - INTERVAL '{{ params.interval }}'
{% endif %}