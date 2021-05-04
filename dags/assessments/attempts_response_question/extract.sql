SELECT  ROW_NUMBER() OVER (
            PARTITION BY  att.district_id,
                          att.id
            ORDER BY      response.ordinality,
                          question.ordinality)                  AS row_number,
        att.district_id,
        id                                                      AS attempt_id,
        att.school_id,
        att.assessment_id,
        att.assignee_id,
        response.ordinality::int                                AS response_order,
        (response.value ->> 'externalItemId')::varchar(100)     AS response_external_item_id,
        question.ordinality::int                                AS question_order,
        (question.value ->> 'score')::numeric(4, 2)             AS question_score,
        (question.value ->> 'maxScore')::numeric(4, 2)          AS question_max_score,
        (question.value ->> 'attempted')::boolean               AS question_attempted,
        (question.value ->> 'questionType')::varchar(50)        AS question_type,
        (question.value ->> 'externalQuestionId')::varchar(100) AS external_question_id,
        (question.value ->> 'externalResponseId')::varchar(100) AS external_response_id,
        att.deleted,
        att.created_at,
        att.updated_at,
        att.deleted_at
FROM    public.attempts AS att,
        LATERAL JSONB_ARRAY_ELEMENTS(att.responses) WITH ORDINALITY response(value),
        LATERAL JSONB_ARRAY_ELEMENTS(response.value -> 'responses') WITH ORDINALITY question(value)
{% if params.columns.audit %}
WHERE   GREATEST({% for column in params.columns.audit -%}{% if loop.index > 1 %}, {% endif -%}{{ column }}{% endfor %}) <  to_timestamp('{{ ts_nodash }}', 'YYYYMMDD"T"HH24MISS')
    AND GREATEST({% for column in params.columns.audit -%}{% if loop.index > 1 %}, {% endif -%}{{ column }}{% endfor %}) >= to_timestamp('{{ ts_nodash }}', 'YYYYMMDD"T"HH24MISS') - INTERVAL '{{ params.interval }}'
{% endif %}