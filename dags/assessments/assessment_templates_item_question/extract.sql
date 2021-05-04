SELECT  ROW_NUMBER() OVER (
            PARTITION BY  atm.district_id,
                          atm.id
            ORDER BY      item.ordinality,
                          question.ordinality)                  AS row_number,
        atm.district_id                                         AS district_id,
        atm.id                                                  AS assessment_template_id,
        item.ordinality::int                                    AS item_order,
        (item.value ->> 'externalId')::varchar(100)             AS item_external_id,
        question.ordinality::int                                AS question_order,
        (question.value ->> 'externalId')::varchar(100)         AS question_external_id,
        (question.value ->> 'type')::varchar(30)                AS question_type,
        (question.value ->> 'rubricReference')::varchar(100)    AS question_rubric_reference,
        (question.value ->> 'maxScore')::numeric(4, 2)          AS question_max_score,
        atm.deleted,
        atm.created_at,
        atm.updated_at,
        atm.deleted_at
FROM    public.assessment_templates atm,
        LATERAL JSONB_ARRAY_ELEMENTS(atm.items) WITH ORDINALITY AS item(value),
        LATERAL JSONB_ARRAY_ELEMENTS(item.value -> 'questions') WITH ORDINALITY AS question(value)
{% if params.columns.audit %}
WHERE   GREATEST({% for column in params.columns.audit -%}{% if loop.index > 1 %}, {% endif -%}{{column}}{% endfor %}) <  to_timestamp('{{ ts_nodash }}', 'YYYYMMDD"T"HH24MISS')
    AND GREATEST({% for column in params.columns.audit -%}{% if loop.index > 1 %}, {% endif -%}{{column}}{% endfor %}) >= to_timestamp('{{ ts_nodash }}', 'YYYYMMDD"T"HH24MISS') - INTERVAL '{{ params.interval }}'
{% endif %}