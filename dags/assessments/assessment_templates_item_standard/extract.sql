SELECT  ROW_NUMBER() OVER (
            PARTITION BY  atm.district_id,
                          atm.id
            ORDER BY      item.ordinality)                                                AS row_number,
        atm.district_id                                                                   AS district_id,
        atm.id                                                                            AS assessment_template_id,
        item.ordinality                                                                   AS item_order,
        unnest(string_to_array(translate(item.value ->> 'standards', '[]\" ', ''), ','))  AS standard,
        atm.deleted,
        atm.created_at,
        atm.updated_at,
        atm.deleted_at
FROM    assessment_templates atm,
        LATERAL jsonb_array_elements(atm.items) with ordinality as item(value)
{% if params.columns.audit %}
WHERE   GREATEST({% for column in params.columns.audit -%}{% if loop.index > 1 %}, {% endif -%}{{column}}{% endfor %}) <  to_timestamp('{{ ts_nodash }}', 'YYYYMMDD"T"HH24MISS')
    AND GREATEST({% for column in params.columns.audit -%}{% if loop.index > 1 %}, {% endif -%}{{column}}{% endfor %}) >= to_timestamp('{{ ts_nodash }}', 'YYYYMMDD"T"HH24MISS') - INTERVAL '{{ params.interval }}'
{% endif %}