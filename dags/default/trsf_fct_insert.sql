INSERT INTO {{ params.destination.schema }}.{{ params.destination.table }} ({% for column in params.columns.all -%}
                                                                            {% if loop.index > 1 %}, {% endif %}{{ column.name }}
                                                                            {% endfor %}
                                                                            , dwh_created_at)
        SELECT  {% for column in params.columns.all -%}
                {% if loop.index > 1 %}, {% endif %}n.{{ column.name }}
                {% endfor %}
                , CURRENT_TIMESTAMP AS dwh_created_at
        FROM    stg_{{ params.destination.table }} AS n
                LEFT JOIN {{ params.destination.schema }}.{{ params.destination.table }} AS o ON
                    {% for column in params.columns.bk -%}
                    {% if loop.index > 1 %} AND {% endif %}o.{{ column }} = n.{{ column }}
                    {% endfor %}
        WHERE   {% for column in params.columns.bk -%}
                {% if loop.index > 1 %} AND {% endif %}o.{{ column }} IS NULL
                {% endfor %}
