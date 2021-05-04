-- UPDATE UNTRACK FIELDS
UPDATE {{ params.destination.schema }}.{{ params.destination.table }} AS trsf
SET    {% for column in params.columns.non_track -%}
            {% if loop.index > 1 %}, {% endif %}{{ column }} = stg.{{ column }}
       {% endfor %}
       , dwh_updated_at          = CURRENT_TIMESTAMP
FROM    stg_{{ params.destination.table }} AS stg
WHERE   {% for column in params.columns.bk -%}
            {% if loop.index > 1 %}AND {% endif %} stg.{{ column }} = trsf.{{ column }}
        {% endfor %}
   -- non track fields
   AND ({% for column in params.columns.non_track -%}
            {% if loop.index > 1 %} OR {% endif %}
            ((stg.{{ column }} IS NULL AND trsf.{{ column }} IS NOT NULL) OR (stg.{{ column }} IS NOT NULL AND trsf.{{ column }} IS NULL) OR (stg.{{ column }} != trsf.{{ column }}))
        {% endfor %})
   AND trsf.dwh_flg_current_version
