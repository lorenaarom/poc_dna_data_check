SELECT  ROW_NUMBER() OVER (
            PARTITION BY  atm.district_id,
                          atm.id
            ORDER BY      item.ordinality)                      AS row_number,
        atm.district_id                                         AS district_id,
        atm.id                                                  AS assessment_template_id,
        item.ordinality :: int                                  AS item_order,
        (item.value ->> 'maxScore') :: numeric(4, 2)            AS max_score,
        (item.value ->> 'externalId') :: varchar(100)           AS external_id,
        (item.value ->> 'scoringType') :: varchar(30)           AS scoring_type,
        (item.value ->> 'ratingRubricVariation') :: varchar(30) AS rating_rubric_variation,
        (item.value ->> 'type') :: varchar(30)                  AS item_type,
        item.value ->> 'title'                                  AS item_title,
        item.value ->> 'description'                            AS description,
        (item.value ->> 'manualScoring') :: boolean             AS manual_scoring,
        item.value ->> 'standards'                              AS standards,
        item.value ->> 'childStandards'                         AS child_standards,
        (item.value ->> 'externalBankItemId') :: varchar(100)   AS external_bank_item_id,
        -- TODO: add audit fields
        atm.deleted,
        atm.created_at,
        atm.updated_at,
        atm.deleted_at
FROM    assessment_templates atm,
        LATERAL JSONB_ARRAY_ELEMENTS(atm.items) WITH ORDINALITY AS item (value)
WHERE   GREATEST({% for column in params.columns.audit -%}{% if loop.index > 1 %}, {% endif -%}{{column}}{% endfor %}) <  to_timestamp('{{ ts_nodash }}', 'YYYYMMDD"T"HH24MISS')
    AND GREATEST({% for column in params.columns.audit -%}{% if loop.index > 1 %}, {% endif -%}{{column}}{% endfor %}) >= to_timestamp('{{ ts_nodash }}', 'YYYYMMDD"T"HH24MISS') - INTERVAL '{{ params.interval }}'
