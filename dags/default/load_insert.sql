INSERT INTO {{ params.destination.schema }}.{{ params.destination.table }} (inserted_by
                                                                            , inserted_at
                                                                            {% for column in params.columns.all -%}
                                                                            , {{ column.name }}
                                                                            {% endfor %})
        SELECT    'AirFlow'           AS inserted_by
                , CURRENT_TIMESTAMP   AS inserted_at
                {% for column in params.columns.all -%}
                , n.{{ column.name }}
                {% endfor %}
        FROM    temp_{{ params.destination.table }}_{{ ts_nodash }} AS n
                LEFT JOIN {{ params.destination.schema }}.{{ params.destination.table }} AS o ON
                    {% for column in params.columns.id -%}
                    {% if loop.index > 1 %} AND {% endif %}o.{{ column }} = n.{{ column }}
                    {% endfor %}
        WHERE   {% for column in params.columns.id -%}
                {% if loop.index > 1 %} AND {% endif %}o.{{ column }} IS NULL
                {% endfor %}
                {% if params.columns.delete %} AND NOT n.{{ params.columns.delete }}{% endif %}
