INSERT INTO {{ params.destination.schema }}.{{ params.destination.table }} (
                              -- BK columns
                              {% for column in params.columns.bk -%}
                                  {{ column }},
                              {% endfor %}
                              -- ALL columns
                              {% for column in params.columns.all -%}
                                  {{ column.name }},
                              {% endfor %}
                              -- Audit columns
                              dwh_flg_current_version,
                              dwh_date_start,
                              dwh_date_end,
                              dwh_created_at)
WITH  trsf_count AS (
    SELECT  {% for column in params.columns.bk -%}
                {% if loop.index > 1 %}, {% endif %} {{ column }}
            {% endfor %}
            , BOOL_OR(dwh_flg_current_version) AS bool_or_dwh_flg_current_version
    FROM    {{ params.destination.schema }}.{{ params.destination.table }}
    GROUP BY
            {% for column in params.columns.bk -%}
                {% if loop.index > 1 %}, {% endif %} {{ column }}
            {% endfor %}
)
SELECT  -- BK columns
        {% for column in params.columns.bk -%}
            stg.{{ column }},
        {% endfor %}
        -- ALL columns
        {% for column in params.columns.all -%}
            stg.{{ column.name }},
        {% endfor %}
        -- Audit columns
        TRUE                                    AS dwh_flg_current_version,
        CASE  WHEN NOT trsf_count.bool_or_dwh_flg_current_version     THEN CURRENT_DATE
              WHEN trsf_count.bool_or_dwh_flg_current_version IS NULL THEN to_date('1900-01-01', 'YYYY-MM-DD')
        END                                     AS dwh_date_start,
        to_date('9999-12-31', 'YYYY-MM-DD')     AS dwh_date_end,
        CURRENT_TIMESTAMP                       AS dwh_created_at
FROM    stg_{{ params.destination.table }} AS stg
        LEFT JOIN trsf_count ON
            {% for column in params.columns.bk -%}
                {% if loop.index > 1 %} AND {% endif %} stg.{{ column }} = trsf_count.{{ column }}
            {% endfor %}
WHERE   NOT COALESCE(trsf_count.bool_or_dwh_flg_current_version, FALSE)