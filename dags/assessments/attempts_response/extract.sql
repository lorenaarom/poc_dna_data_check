SELECT  ROW_NUMBER() OVER (
            PARTITION BY  att.district_id,
                          att.id
            ORDER BY      response.ordinality)                  AS row_number,
        att.district_id,
        att.id                                                  AS attempt_id,
        att.school_id,
        att.assessment_id,
        att.assignee_id,
        response.ordinality::int                                AS response_order,
        (response.value ->> 'score')::numeric(4, 2)             AS score,
        (response.value ->> 'maxScore')::numeric(4, 2)          AS max_score,
        (response.value ->> 'attempted')::boolean               AS attempted,
        (response.value ->> 'externalItemId')::varchar(100)     AS external_item_id,
        (response.value ->> 'type')::varchar(20)                AS type,
        (response.value ->> 'timeSpent')::numeric(10, 2)        AS time_spent,
        --
        att.deleted,
        --
        att.created_at,
        att.updated_at,
        att.deleted_at
FROM    attempts att,
        LATERAL JSONB_ARRAY_ELEMENTS(att.responses) WITH ORDINALITY RESPONSE(value)
{% if params.columns.audit %}
WHERE   GREATEST({% for column in params.columns.audit -%}{% if loop.index > 1 %}, {% endif -%}{{column}}{% endfor %}) <  to_timestamp('{{ ts_nodash }}', 'YYYYMMDD"T"HH24MISS')
    AND GREATEST({% for column in params.columns.audit -%}{% if loop.index > 1 %}, {% endif -%}{{column}}{% endfor %}) >= to_timestamp('{{ ts_nodash }}', 'YYYYMMDD"T"HH24MISS') - INTERVAL '{{ params.interval }}'
{% endif %}