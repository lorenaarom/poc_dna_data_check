CREATE TEMP TABLE stg_dim_standard_question AS
  SELECT s.id ||'$'||s.standard_global_id ||'$'|| t.assessment_template_id || '$' ||
         s.external_item_id               AS bk_dim_standard_question,
         COALESCE(ds.sk_dim_standard, -1) AS sk_dim_standard,
         COALESCE(dq.sk_dim_question, -1) AS sk_dim_question,
         s.external_item_id               AS id_item_external,
         s.standard_set_global_id         AS id_standard_set_global,
         s.hierarchy_level                AS des_standard_item_hierarchy_level,
         s.deleted                        AS flg_deleted,
         'AFFIRM'                         AS dwh_source_system_cd,
         'ITEM_STANDARDS'                 AS dwh_source_data_cd
  FROM assessments.item_standards s
         INNER JOIN assessments.assessment_templates_item as t on s.external_item_id = t.external_id
         LEFT OUTER JOIN edw.dim_question AS dq
           ON dq.bk_dim_question = t.assessment_template_id || '$' || s.external_item_id || '$0'
                and dq.dwh_flg_current_version
         LEFT OUTER JOIN edw.dim_standard AS ds ON ds.id_standard_item_global = s.standard_global_id
                                                     and ds.dwh_flg_current_version and
                                                   flg_last_version_id_standard_item_global
