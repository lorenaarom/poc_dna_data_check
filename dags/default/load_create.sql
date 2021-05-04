CREATE TEMP TABLE temp_{{ params.destination.table }}_{{ ts_nodash }}
(LIKE {{ params.destination.schema }}.{{ params.destination.table }})