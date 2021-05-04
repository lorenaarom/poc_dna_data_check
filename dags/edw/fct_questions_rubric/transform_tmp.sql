CREATE TEMP TABLE tmp_fct_questions_rubric AS
   SELECT qr.id || '$' ||qrr.item_key 							     AS bk_fct_questions_rubric,
         a.id                                                        AS id_assessment,
         att.ID                                                      AS id_attempt,
         qr.id												  	     AS id_questions_rubric,
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
         t.id || '$' || qr.external_item_id  || '$0'                 AS bk_dim_question,       --bk = dim_question.bk
         case when ati.scoring_type ='rating'
		 then replace (ati.rating_rubric_variation,'Variation ','') || '$' || qrr.item_key
    	 else null end 												 AS bk_dim_rating_variation, --bk = dim_rating_variation.bk,
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
         qrr.item_key 												 AS questions_rubric_item_key,
		 qrr.item_value      	  									 AS questions_rubric_item_value,
         a.created_at                                                AS assessment_created_at,
         a.updated_at                                                AS assessment_updated_at,
         att.created_at                                              AS attempt_created_at,
         att.updated_at                                              AS attempt_updated_at,
		 qr.created_at 												 AS questions_rubric_created_at,
		 qr.updated_at 												 AS questions_rubric_updated_at,
		 ati.scoring_type                                            AS tmp_scoring_type,
         trunc(a.start_date)                                         AS tmp_date_assessment_start
  from assessments.assessments a
		INNER JOIN assessments.attempts att on a.id = att.assessment_id
		INNER JOIN assessments.questions_rubric qr on qr.attempt_id = att.id
		INNER JOIN assessments.questions_rubric_responses qrr on qr.id = qrr.questions_rubric_id
		INNER JOIN assessments.assessment_templates t on t.id = a.assessment_template_id
		INNER JOIN assessments.assessment_templates_item ati on t.id = ati.assessment_template_id
				   AND ati.external_id = qr.external_item_id
WHERE GREATEST( a.created_at ,a.updated_at ,a.deleted_at,
                   qr.created_at ,qr.updated_at ,qr.deleted_at ,
                   qrr.created_at ,qrr.updated_at ,qrr.deleted_at) >  to_timestamp('{{ ts_nodash }}', 'YYYYMMDD"T"HH24MISS')
    AND GREATEST( a.created_at ,a.updated_at ,a.deleted_at,
                   qr.created_at ,qr.updated_at ,qr.deleted_at ,
                   qrr.created_at ,qrr.updated_at ,qrr.deleted_at) <= to_timestamp('{{ ts_nodash }}', 'YYYYMMDD"T"HH24MISS') + INTERVAL '{{ params.interval }}'
;
