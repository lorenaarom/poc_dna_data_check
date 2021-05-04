COPY        temp_{{ params.destination.table }}_{{ ts_nodash }}({% for column in params.columns.all -%}
                                                                {% if loop.index > 1 %}, {% endif %}{{ column.name }}
                                                                {% endfor %}
                                                                {% if params.columns.delete %}, {{ params.columns.delete }}{% endif %})
FROM        's3://{{ var.value.bucket_name }}/airflow/{{ ds }}/{{ params.destination.schema}}/{{ params.destination.table }}/{{ ts_nodash }}/'
IAM_ROLE    '{{ var.value.iam_role }}'
FORMAT AS   CSV DELIMITER ',' IGNOREHEADER 1 GZIP
REGION      '{{ var.value.region }}'