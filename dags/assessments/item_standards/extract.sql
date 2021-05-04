SELECT id,
	external_item_id,
	standard_global_ids,
	unnest(standard_global_ids) AS standard_global_id,
	ROW_NUMBER () OVER (
		PARTITION BY external_item_id
		ORDER BY updated_at,
			unnest(standard_global_ids)
	) standard_global_id_order,
	standard_set_global_id,
	hierarchy_level,
	deleted,
	created_by,
	updated_by,
	deleted_by,
	created_at,
	updated_at,
	deleted_at
FROM item_standards its
{% if params.columns.audit %}
WHERE	GREATEST({% for column in params.columns.audit -%}{% if loop.index > 1 %}, {% endif -%}its.{{ column }}{% endfor %}) <  to_timestamp('{{ ts_nodash }}', 'YYYYMMDD"T"HH24MISS')
    AND GREATEST({% for column in params.columns.audit -%}{% if loop.index > 1 %}, {% endif -%}its.{{ column }}{% endfor %}) >= to_timestamp('{{ ts_nodash }}', 'YYYYMMDD"T"HH24MISS') - INTERVAL '{{ params.interval }}'
{% endif %}
