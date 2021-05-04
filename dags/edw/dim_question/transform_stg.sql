CREATE TEMP TABLE stg_dim_question AS
  SELECT at.id || '$' || ati.external_id || '$0'          AS bk_dim_question,
         COALESCE(dd.sk_dim_district, -1)                 AS sk_dim_district,
         COALESCE(dc.sk_dim_curriculum, -1)               AS sk_dim_curriculum,
         at.id                                            AS id_template,
         at.parent_id                                     AS id_template_parent_id,
         at.external_id                                   AS id_template_external,
         at.title                                         AS des_template,
         at.assignment_type                               AS des_template_assignment_type,
         at.grade                                         AS des_template_grade,
         at.module                                        AS des_template_module,
         at.lesson                                        AS des_template_lesson,
         at.curriculum                                    AS des_template_curriculum,
         at.topic                                         AS des_template_topic,
         ati.external_id                                  AS id_item_external,
         ati.external_bank_item_id                        AS id_item_external_bank,
         ati.item_order                                   AS id_item_order,
         DENSE_RANK() OVER (
           PARTITION BY at.id, COALESCE((ati.item_type) :: VARCHAR(30), 'core')
           ORDER BY ati.item_order)                       AS id_item_order_type,
         COALESCE((ati.item_type) :: VARCHAR(50), 'core') AS des_item_type,
         ati.scoring_type                                 AS des_item_scoring_type,
         ati.item_title                                   AS des_item_title,
         ati.description                                  AS des_item_description,
         ati.max_score                                    AS des_item_max_score,
          case when COALESCE(ati.item_type, 'core') ='core'
              then ati.max_score
              else 0 end                                  AS des_item_max_score_core,
         ati.rating_rubric_variation                      AS des_item_rating_rubric_variation,
         ati.manual_scoring                               AS flg_item_manual_scoring,
         0                                                AS id_question_order,
         'Not Apply' :: VARCHAR(100)                      AS id_question_external,
         'Not Apply'                                      AS des_question_type,
         'Not Apply'                                      AS des_question_rubric_reference,
         NULL                                             AS des_question_max_score,
         NULL                                             AS des_question_max_score_core,
         at.deleted                                       AS flg_template_deleted,
         'AFFIRM'                                         AS dwh_source_system_cd,
         'ASSESSMENT_TEMPLATES'                           AS dwh_source_data_cd
  FROM assessments.assessment_templates at
         INNER JOIN assessments.assessment_templates_item ati ON at.id = ati.assessment_template_id
         LEFT OUTER JOIN edw.dim_district as dd ON dd.bk_dim_district = at.district_id and dd.dwh_flg_current_version
         LEFT OUTER JOIN edw.dim_curriculum as dc
           ON dc.id_curriculum_global = at.curriculum and dc.dwh_flg_current_version and
              dc.flg_last_version_id_curriculum_global
  UNION ALL
  SELECT at.id || '$' || ati.external_id || '$' || atiq.question_external_id AS bk_dim_question,
         COALESCE(dd.sk_dim_district, -1)                                    AS sk_dim_district,
         COALESCE(dc.sk_dim_curriculum, -1)                                  AS sk_dim_curriculum,
         at.id                                                               AS id_template,
         at.parent_id                                                        AS id_template_parent_id,
         at.external_id                                                      AS id_template_external,
         at.title                                                            AS des_template,
         at.assignment_type                                                  AS des_template_assignment_type,
         at.grade                                                            AS des_template_grade,
         at.module                                                           AS des_template_module,
         at.lesson                                                           AS des_template_lesson,
         at.curriculum                                                       AS des_template_curriculum,
         at.topic                                                            AS des_template_topic,
         ati.external_id                                                     AS id_item_external,
         ati.external_bank_item_id                                           AS id_item_external_bank,
         ati.item_order                                                      AS id_item_order,
         DENSE_RANK() OVER (
           PARTITION BY at.id, COALESCE(ati.item_type, 'core')
           ORDER BY ati.item_order)                                          AS id_item_order_type,
         COALESCE(ati.item_type, 'core')                                     AS des_item_type,
         ati.scoring_type                                                    AS des_item_scoring_type,
         ati.item_title                                                      AS des_item_title,
         ati.description                                                     AS des_item_description,
         NULL                                                                AS des_item_max_score,
         NULL                                                                AS des_item_max_score_core,
         ati.rating_rubric_variation                                         AS des_item_rating_rubric_variation,
         ati.manual_scoring                                                  AS flg_item_manual_scoring,
         atiq.question_order                                                 AS id_question_order,
         atiq.question_external_id                                           AS id_question_external,
         atiq.question_type                                                  AS des_question_type,
         atiq.question_rubric_reference                                      AS des_question_rubric_reference,
         atiq.question_max_score                                             AS des_question_max_score,
          case when COALESCE(ati.item_type, 'core') ='core'
              then atiq.question_max_score
              else 0 end                                                     AS des_question_max_score_core,
         at.deleted                                                          AS flg_template_deleted,
         'AFFIRM'                                                            AS dwh_source_system_cd,
         'ASSESSMENT_TEMPLATES'                                              AS dwh_source_data_cd
  FROM assessments.assessment_templates at
         INNER JOIN assessments.assessment_templates_item ati ON at.id = ati.assessment_template_id
         INNER JOIN assessments.assessment_templates_item_question atiq
           ON at.id = atiq.assessment_template_id AND ati.item_order = atiq.item_order
         LEFT OUTER JOIN edw.dim_district AS dd ON dd.bk_dim_district = at.district_id and dd.dwh_flg_current_version
         LEFT OUTER JOIN edw.dim_curriculum as dc
           ON dc.id_curriculum_global = at.curriculum and dc.dwh_flg_current_version and
              dc.flg_last_version_id_curriculum_global
