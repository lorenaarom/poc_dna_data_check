SELECT  att.id,
        att.district_id,
        att.school_id,
        att.assessment_id,
        att.assignee_id,
        COALESCE(nullif(att.score,'NaN'),0) AS score,
        att.max_score,
        att.platform_id,
        att.started_at,
        att.closed_at,
        att.deleted,
        att.created_at,
        att.updated_at,
        att.deleted_at,
        att.status,
        att.latest,
        att.graded,
        att.completed_on_paper
FROM    attempts AS att
        INNER JOIN assessments AS ast ON
                ast.id = att.assessment_id
            AND ast.status = 'done'
            AND NOT ast.deleted /* ast.deleted = false */
WHERE   NOT att.deleted /* att.deleted = false */
{% if params.columns.audit %}
    AND GREATEST({% for column in params.columns.audit -%}{% if loop.index > 1 %}, {% endif -%}att.{{ column }}{% endfor %}) <  to_timestamp('{{ ts_nodash }}', 'YYYYMMDD"T"HH24MISS')
    AND GREATEST({% for column in params.columns.audit -%}{% if loop.index > 1 %}, {% endif -%}att.{{ column }}{% endfor %}) >= to_timestamp('{{ ts_nodash }}', 'YYYYMMDD"T"HH24MISS') - INTERVAL '{{ params.interval }}'
{% endif %}