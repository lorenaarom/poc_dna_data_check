{% if 'delete' in params.get('columns') %}
ALTER TABLE temp_{{ params.destination.table }}_{{ ts_nodash }} ADD COLUMN {{ params.columns.delete }} BOOLEAN;
{% endif %}

ALTER TABLE temp_{{ params.destination.table }}_{{ ts_nodash }} DROP COLUMN inserted_by;
ALTER TABLE temp_{{ params.destination.table }}_{{ ts_nodash }} DROP COLUMN inserted_at;
