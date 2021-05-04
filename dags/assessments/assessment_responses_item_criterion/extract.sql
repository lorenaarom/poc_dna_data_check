SELECT
	ROW_NUMBER() OVER ()::INT AS row_number,
	ast.assessment_template_id,
	ast.id AS assessment_id,
	ast.district_id,
	att.id as attempt_id,
	ati.item_order,
	qr.id AS question_id,
	(qr.value ->> 'key')::INT AS criterion_key,
	(qr.value ->> 'value')::INT AS criterion_value,
	att.created_at,
	att.updated_at,
	att.deleted_at,
	att.deleted
FROM (SELECT
		atm.id AS assessment_template_id,
		item.ORDINALITY::INT AS item_order,
		(item.value ->> 'externalId')::VARCHAR(100) AS external_id
	FROM
		assessment_templates atm,
		LATERAL JSONB_ARRAY_ELEMENTS(atm.items) WITH ORDINALITY AS item(value)
	WHERE
		atm.deleted = FALSE
	AND (item.value ->> 'scoringType')::varchar(30) = 'matrix') ati
INNER JOIN assessment_templates atm ON ati.assessment_template_id = atm.id
INNER JOIN assessments ast ON atm.id = ast.assessment_template_id
INNER JOIN attempts att ON ast.id = att.assessment_id
INNER JOIN (SELECT
		DISTINCT ON (qr1.attempt_id,
		qr1.external_item_id,
		res.ORDINALITY::INT)
		qr1.id,
		qr1.attempt_id,
		qr1.external_item_id,
		qr1.external_question_id,
		qr1.responses,
		qr1.deleted,
		qr1.created_at,
		qr1.updated_at,
		qr1.deleted_at,
		res.ORDINALITY::INT AS response_order,
		res.value
	FROM
		questions_rubric qr1,
	LATERAL JSONB_ARRAY_ELEMENTS(qr1.responses) WITH ORDINALITY res(value)
	WHERE
		qr1.deleted = FALSE -- and attempt_id = '9de0a72d-0507-4bea-9579-e4c7625d4d7c'
	ORDER BY qr1.attempt_id,
		qr1.external_item_id,
		res.ORDINALITY::INT,
		qr1.updated_at DESC
	) qr
ON att.id = qr.attempt_id
AND ati.external_id = qr.external_item_id
WHERE atm.deleted = FALSE
	AND ast.status = 'done'
	AND ast.deleted = FALSE
    AND GREATEST({% for column in params.columns.audit -%}{% if loop.index > 1 %}, {% endif -%}att.{{ column }}{% endfor %}) <  to_timestamp('{{ ts_nodash }}', 'YYYYMMDD"T"HH24MISS')
    AND GREATEST({% for column in params.columns.audit -%}{% if loop.index > 1 %}, {% endif -%}att.{{ column }}{% endfor %}) >= to_timestamp('{{ ts_nodash }}', 'YYYYMMDD"T"HH24MISS') - INTERVAL '{{ params.interval }}'
