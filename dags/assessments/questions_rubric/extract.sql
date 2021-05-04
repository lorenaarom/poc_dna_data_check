SELECT
	DISTINCT ON
	(qr.attempt_id,
	qr.external_item_id) qr.id,
	att.district_id,
	qr.attempt_id,
	qr.external_item_id,
	qr.external_question_id,
	qr.responses,
	qr.deleted,
	qr.created_at,
	qr.updated_at,
	qr.deleted_at
FROM
	questions_rubric qr
JOIN attempts att ON
	qr.attempt_id = att.id
{% if params.columns.audit %}
WHERE	GREATEST({% for column in params.columns.audit -%}{% if loop.index > 1 %}, {% endif -%}qr.{{ column }}{% endfor %}) <  to_timestamp('{{ ts_nodash }}', 'YYYYMMDD"T"HH24MISS')
    AND GREATEST({% for column in params.columns.audit -%}{% if loop.index > 1 %}, {% endif -%}qr.{{ column }}{% endfor %}) >= to_timestamp('{{ ts_nodash }}', 'YYYYMMDD"T"HH24MISS') - INTERVAL '{{ params.interval }}'
{% endif %}
