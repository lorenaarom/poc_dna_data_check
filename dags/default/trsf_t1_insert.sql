INSERT INTO {{ params.destination.schema }}.{{ params.destination.table }} (
                              -- BK columns
                              {% for column in params.columns.bk %}
                                  {{ column }},
                              {% endfor %}
                              -- ALL columns
                              {% for column in params.columns.all %}
                                  {{ column.name }},
                              {% endfor %}
                              -- Audit columns
                              dwh_created_at)
SELECT  -- BK columns
        {% for column in params.columns.bk -%}
            stg.{{ column }},
        {% endfor %}
        -- ALL columns
        {% for column in params.columns.all -%}
            stg.{{ column.name }},
        {% endfor %}
        -- Audit columns
        CURRENT_TIMESTAMP AS dwh_created_at
FROM    stg_{{ params.destination.table }} AS stg
        LEFT JOIN {{ params.destination.schema }}.{{ params.destination.table }} AS trsf ON
            {% for column in params.columns.bk -%}
                {% if loop.index > 1 %} AND {% endif %} stg.{{ column }} = trsf.{{ column }}
            {% endfor %}
WHERE   {% for column in params.columns.bk -%}
            {% if loop.index > 1 %} AND {% endif %}
            trsf.{{ column }} IS NULL
        {% endfor %}
