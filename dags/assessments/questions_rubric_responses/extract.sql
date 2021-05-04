SELECT ROW_NUMBER() OVER ()::INT AS row_number,
			att.district_id,
			qr.id AS questions_rubric_id,
			qr.external_item_id,
			qr.external_question_id,
			qr.attempt_id,
			(questions_rubric_item.value->>'key')::INT AS item_key,
			(questions_rubric_item.value->>'value')::INT AS item_value,
			qr.created_at,
			qr.updated_at,
			qr.deleted_at,
			qr.deleted
		FROM questions_rubric qr join attempts att on qr.attempt_id = att.id,
			lateral jsonb_array_elements(qr.responses) WITH ORDINALITY AS questions_rubric_item(value)
{% if params.columns.audit %}
WHERE	GREATEST({% for column in params.columns.audit -%}{% if loop.index > 1 %}, {% endif -%}qr.{{ column }}{% endfor %}) <  to_timestamp('{{ ts_nodash }}', 'YYYYMMDD"T"HH24MISS')
    AND GREATEST({% for column in params.columns.audit -%}{% if loop.index > 1 %}, {% endif -%}qr.{{ column }}{% endfor %}) >= to_timestamp('{{ ts_nodash }}', 'YYYYMMDD"T"HH24MISS') - INTERVAL '{{ params.interval }}'
{% endif %}
