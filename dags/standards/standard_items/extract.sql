SELECT  id
        , global_id
        , provider_id
        , code
        , description
        , subject
        , subject_provider_id
        , course
        , course_provider_id
        , standard_set_id
        , hierarchy_level
        , hierarchy_level_order
        , ancestors_global_ids
        , ancestors_global_ids[1] as  ancestors_global_id
        , created_at
        , updated_at
        , deleted_at
        , created_by
        , updated_by
        , deleted_by
        , deleted
        , short_code
FROM    public.standard_items
{% if params.columns.audit %}
WHERE   GREATEST({% for column in params.columns.audit -%}{% if loop.index > 1 %}, {% endif -%}{{ column }}{% endfor %}) <  to_timestamp('{{ ts_nodash }}', 'YYYYMMDD"T"HH24MISS')
    AND GREATEST({% for column in params.columns.audit -%}{% if loop.index > 1 %}, {% endif -%}{{ column }}{% endfor %}) >= to_timestamp('{{ ts_nodash }}', 'YYYYMMDD"T"HH24MISS') - INTERVAL '{{ params.interval }}'
{% endif %}