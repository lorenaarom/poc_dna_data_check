SELECT
	DISTINCT ROW_NUMBER() OVER ()::INT AS row_number,
	atm.id AS assessment_template_id,
	atm.title,
	ast.id AS assessment_id,
	ast.district_id,
	att.id AS attempt_id,
	att.assignee_id AS assignee_id,
	ati.external_id AS item_external_id,
	ati.item_order AS item_order,
	ati.scoring_type AS item_score_type,
	ati.rating_rubric_variation AS item_variation,
	(SELECT COUNT(*)
		FROM JSONB_ARRAY_ELEMENTS(qr.responses) question_responses(value)
		WHERE (question_responses.value ->> 'value')::INT = 0
	) AS response_score,
	ar.max_score AS response_max_score,
	qr.id AS question_id,
	CASE
		WHEN ati.scoring_type = 'rating' THEN (jsonb_array_element(qr.responses, 0) ->> 'value')::INT
	END AS category_1,
	CASE
		WHEN ati.scoring_type = 'rating' THEN (jsonb_array_element(qr.responses, 1) ->> 'value')::INT
	END AS category_2,
	CASE
		WHEN ati.scoring_type = 'rating' THEN (jsonb_array_element(qr.responses,
		2) ->> 'value')::INT
	END AS category_3,
	CASE
		WHEN ati.scoring_type = 'rating' THEN (jsonb_array_element(qr.responses,
		3) ->> 'value')::INT
	END AS category_4,
	CASE
		WHEN ati.scoring_type = 'rating' THEN (jsonb_array_element(qr.responses,
		4) ->> 'value')::INT
	END AS category_5,
	CASE
		WHEN ati.scoring_type = 'rating' THEN (jsonb_array_element(qr.responses,
		5) ->> 'value')::INT
	END AS category_6,
	qr.responses AS question_responses,
	att.created_at,
	att.updated_at,
	att.deleted_at,
	att.deleted
FROM
	assessment_templates atm
JOIN assessments ast ON
	atm.id = ast.assessment_template_id
JOIN (
	SELECT
		ROW_NUMBER() OVER ()::INT AS row_number,
		atm.district_id,
		atm.id AS assessment_template_id,
		item.ordinality::INT AS item_order,
		(item.value ->> 'maxScore')::numeric(4,
		2) AS max_score,
		(item.value ->> 'externalId')::varchar(100) AS external_id,
		(item.value ->> 'scoringType')::varchar(30) AS scoring_type,
		(item.value ->> 'ratingRubricVariation')::varchar(30) AS rating_rubric_variation,
		(item.value ->> 'type')::varchar(30) AS item_type,
		item.value ->> 'title' AS item_title,
		item.value ->> 'description' AS description,
		(item.value ->> 'manualScoring')::boolean AS manual_scoring,
		item.value ->> 'standards' AS standards,
		item.value ->> 'childStandards' AS child_standards,
		(item.value ->> 'externalBankItemId')::varchar(100) AS external_bank_item_id
	FROM
		assessment_templates atm,
		LATERAL JSONB_ARRAY_ELEMENTS(atm.items) WITH ORDINALITY AS item(value)
	WHERE
		atm.deleted = FALSE) ati ON
	atm.id = ati.assessment_template_id
JOIN attempts att ON
	ast.id = att.assessment_id
JOIN (
	SELECT
		ROW_NUMBER() OVER ()::INT AS row_number,
		att.district_id,
		id AS attempt_id,
		att.school_id,
		att.assessment_id,
		att.assignee_id,
		response.ORDINALITY::INT AS response_order,
		(response.value ->> 'score')::NUMERIC(4, 2) AS score,
		(response.value ->> 'maxScore')::NUMERIC(4, 2) AS max_score,
		(response.value ->> 'attempted')::BOOLEAN AS attempted,
		(response.value ->> 'externalItemId')::VARCHAR(100) AS external_item_id,
		(response.value ->> 'type')::VARCHAR(20) AS type,
		(response.value ->> 'timeSpent')::NUMERIC(4, 2) AS time_spent
	FROM
		attempts att,
		LATERAL JSONB_ARRAY_ELEMENTS(att.responses) WITH ORDINALITY response(value)) ar ON
	att.id = ar.attempt_id
	AND ati.external_id = ar.external_item_id
JOIN (
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
	WHERE
		qr.deleted = FALSE
	ORDER BY
		attempt_id,
		external_item_id,
		updated_at DESC) qr ON
	att.id = qr.attempt_id
	AND ati.external_id = qr.external_item_id
WHERE
	atm.deleted = FALSE
	AND ast.status = 'done'
	AND ast.deleted = FALSE
	AND GREATEST({% for column in params.columns.audit -%}{% if loop.index > 1 %}, {% endif -%}att.{{ column }}{% endfor %}) <  to_timestamp('{{ ts_nodash }}', 'YYYYMMDD"T"HH24MISS')
    AND GREATEST({% for column in params.columns.audit -%}{% if loop.index > 1 %}, {% endif -%}att.{{ column }}{% endfor %}) >= to_timestamp('{{ ts_nodash }}', 'YYYYMMDD"T"HH24MISS') - INTERVAL '{{ params.interval }}'
