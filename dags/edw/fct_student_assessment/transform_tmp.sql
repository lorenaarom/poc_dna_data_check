CREATE TEMP TABLE tmp_fct_student_assessment AS
  ---+ FACT TABLE QUESTION ITEM LEVEL
  SELECT   att.ID || '$' || ar.external_item_id || '$0' AS bk_fct_student_assessment,
           a.id                                                        AS id_assessment,
           att.ID                                                      AS id_attempt,
           t.id                                                        AS id_template,
           a.platform_id                                               AS id_platform,
           TO_CHAR(a.start_date, 'YYYYMMDD') :: int                    AS sk_date_assessment_start,
           TO_CHAR(a.end_date, 'YYYYMMDD') :: int                      AS sk_date_assessment_end,
           TO_CHAR(a.assigned_at, 'YYYYMMDD') :: int                   AS sk_date_assessment_assigned,
           TO_CHAR(a.finished_at, 'YYYYMMDD') :: int                   AS sk_date_assessment_finished,
           TO_CHAR(att.started_at, 'YYYYMMDD') :: int                  AS sk_date_attempt_start,
           TO_CHAR(att.closed_at, 'YYYYMMDD') :: int                   AS sk_date_attempt_closed,
           a.district_id                                               AS bk_dim_district,       --bk = district.id
           att.assignee_id                                             AS id_student_global,     --bk = student.global_id
           a.owner_id                                                  AS id_teacher_global,     --bk = teacher.global_id
           a.context_id                                                AS id_classroom_global,   --bk = classrrom.global_id
           a.enrollment_period_id                                      AS bk_dim_school_year,    --bk = enrollment_period.id
           a.platform_id                                               AS id_product_global,     --bk = product.global_id
           t.curriculum                                                AS id_curriculum_global,  --bk = curriculum.global_id
           t.id || '$' || ar.external_item_id || '$0'                  AS bk_dim_question,       --bk = dim_question.bk
           a.type                                                      AS des_assessment_type,
           a.status                                                    AS des_assessment_status,
           a.auto_launch                                               AS flg_assessment_auto_launch,
           a.deleted                                                   AS flg_asessment_deleted,
           att.status                                                  AS des_attempt_status,
           att.latest                                                  AS flg_attempt_latest,
           att.deleted                                                 AS flg_attempt_deleted,
           att.graded                                                  AS flg_attempt_graded,
           att.completed_on_paper                                      AS flg_attempt_completed_on_paper,
           att.max_score                                               AS des_attempt_max_score,
           ar.attempted                                                AS response_item_flg_attempted,
           COALESCE(ar.type, 'core') :: VARCHAR(30)                    AS response_item_type,
           ar.score                                                    AS response_item_score,
           CASE COALESCE(ar.type, 'core') :: VARCHAR(30)
             WHEN 'core'
                     THEN ar.score
             ELSE 0
               END                                                     AS response_item_score_core,
           ar.max_score                                                AS response_item_max_score,
           CASE COALESCE((ar.type), 'core') :: VARCHAR(30)
             WHEN 'core'
                     THEN ar.max_score
             ELSE 0
               END                                                     AS response_item_max_score_core,
           ar.time_spent                                               AS response_item_time_spent,
           NULL                                                        AS response_question_flg_attempted,
           NULL                                                        AS response_question_des_question_type,
           NULL                                                        AS response_question_score,
           NULL                                                        AS response_question_score_core,
           NULL                                                        AS response_question_max_score,
           NULL                                                        AS response_question_max_score_core,
           a.created_at                                                AS assessment_created_at,
           a.updated_at                                                AS assessment_updated_at,
           att.created_at                                              AS attempt_created_at,
           att.updated_at                                              AS attempt_updated_at,
           trunc(a.start_date)                                         AS tmp_date_assessment_start
  FROM assessments.assessments a
         INNER JOIN assessments.attempts att ON a.id = att.assessment_id
         INNER JOIN assessments.assessment_templates t ON t.id = a.assessment_template_id
         INNER JOIN assessments.attempts_response ar ON att.id = ar.attempt_id
   WHERE GREATEST( a.created_at ,a.updated_at ,a.deleted_at,
                   att.created_at ,att.updated_at ,att.deleted_at ,
                   ar.created_at ,ar.updated_at ,ar.deleted_at) > to_timestamp('{{ ts_nodash }}', 'YYYYMMDD"T"HH24MISS')
    AND GREATEST( a.created_at ,a.updated_at ,a.deleted_at,
                  att.created_at ,att.updated_at ,att.deleted_at ,
                   ar.created_at ,ar.updated_at ,ar.deleted_at) <= to_timestamp('{{ ts_nodash }}', 'YYYYMMDD"T"HH24MISS') + INTERVAL '{{ params.interval }}'
  UNION ALL
  ---+  QUESTION LEVEL
  SELECT   att.ID || '$' || ar.external_item_id || '$'||  arq.external_question_id   AS bk_fct_student_assessment,
           a.id                                                                 AS id_assessment,
           att.ID                                                               AS id_attempt,
           t.id                                                       			AS id_template,
           a.platform_id                                               			AS id_platform,
           TO_CHAR(a.start_date, 'YYYYMMDD') :: int                             AS sk_date_assessment_start,
           TO_CHAR(a.end_date, 'YYYYMMDD') :: int                               AS sk_date_assessment_end,
           TO_CHAR(a.assigned_at, 'YYYYMMDD') :: int                            AS sk_date_assessment_assigned,
           TO_CHAR(a.finished_at, 'YYYYMMDD') :: int                            AS sk_date_assessment_finished,
           TO_CHAR(att.started_at, 'YYYYMMDD') :: int                           AS sk_date_attempt_start,
           TO_CHAR(att.closed_at, 'YYYYMMDD') :: int                            AS sk_date_attempt_closed,
           a.district_id                                                        AS bk_dim_district,       --bk = district.id
           att.assignee_id                                                      AS id_student_global,     --bk = student.global_id
           a.owner_id                                                           AS id_teacher_global,     --bk = teacher.global_id
           a.context_id                                                         AS id_classroom_global,   --bk = classrrom.global_id
           a.enrollment_period_id                                               AS bk_dim_school_year,    --bk = enrollment_period.id
           a.platform_id                                                        AS id_product_global,     --bk = product.global_id
           t.curriculum                                                         AS id_curriculum_global,  --bk = curriculum.global_id
           t.id || '$' || ar.external_item_id || '$'|| arq.external_question_id AS bk_dim_question,       --bk = dim_question.bk
           a.type                                                               AS des_assessment_type,
           a.status                                                             AS des_assessment_status,
           a.auto_launch                                                        AS flg_assessment_auto_launch,
           a.deleted                                                            AS flg_asessment_deleted,
           att.status                                                           AS des_attempt_status,
           att.latest                                                           AS flg_attempt_latest,
           att.deleted                                                          AS flg_attempt_deleted,
           att.graded                                                           AS flg_attempt_graded,
           att.completed_on_paper                                               AS flg_attempt_completed_on_paper,
           att.max_score                                                        AS des_attempt_max_score,
           ar.attempted                                                         AS response_item_flg_attempted,
           COALESCE(ar.type, 'core') :: VARCHAR(30)                             AS response_item_type,
           NULL                                                                 AS response_item_score,
           NULL                                                                 AS response_item_score_core,
           NULL                                                                 AS response_item_max_score,
           NULL                                                                 AS response_item_max_score_core,
           NULL                                                                 AS response_item_time_spent,
           ar.attempted                                                         AS response_question_flg_attempted,
           arq.question_type                                                    AS response_question_des_question_type,
           arq.question_score                                                   AS response_question_score,
           CASE COALESCE(ar.type, 'core') :: VARCHAR(30)
             WHEN 'core'
                     THEN arq.question_score
             ELSE 0
               END                                                              AS response_question_score_core,
           arq.question_max_score                                               AS response_question_max_score,
           CASE COALESCE(ar.type, 'core') :: VARCHAR(30)
             WHEN 'core'
                     THEN arq.question_max_score
             ELSE 0
               END                                                              AS response_question_max_score_core,
           a.created_at                                                         AS assessment_created_at,
           a.updated_at                                                         AS assessment_updated_at,
           att.created_at                                                       AS attempt_created_at,
           att.updated_at                                                       AS attempt_updated_at,
           trunc(a.start_date)                                                  AS tmp_date_assessment_start
  FROM assessments.assessments a
         INNER JOIN assessments.attempts att ON a.id = att.assessment_id
         INNER JOIN assessments.assessment_templates t ON t.id = a.assessment_template_id
         INNER JOIN assessments.attempts_response ar ON att.id = ar.attempt_id
         INNER JOIN assessments.attempts_response_question arq
           on ar.attempt_id = arq.attempt_id AND ar.external_item_id = arq.response_external_item_id
    WHERE GREATEST( a.created_at ,a.updated_at ,a.deleted_at,
                   att.created_at ,att.updated_at ,att.deleted_at ,
                   ar.created_at ,ar.updated_at ,ar.deleted_at,
                   arq.created_at ,arq.updated_at ,arq.deleted_at) >  to_timestamp('{{ ts_nodash }}', 'YYYYMMDD"T"HH24MISS')
    AND GREATEST( a.created_at ,a.updated_at ,a.deleted_at,
                  att.created_at ,att.updated_at ,att.deleted_at ,
                   ar.created_at ,ar.updated_at ,ar.deleted_at,
                   arq.created_at ,arq.updated_at ,arq.deleted_at) <= to_timestamp('{{ ts_nodash }}', 'YYYYMMDD"T"HH24MISS') + INTERVAL '{{ params.interval }}'
