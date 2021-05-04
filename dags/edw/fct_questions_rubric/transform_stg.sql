CREATE TEMP TABLE stg_fct_questions_rubric AS
SELECT  	tmp.bk_fct_questions_rubric,
			tmp.id_assessment,
			tmp.id_attempt,
			tmp.id_template,
			tmp.id_platform,
			tmp.id_questions_rubric,
			tmp.sk_date_assessment_start,
			tmp.sk_date_assessment_end,
			tmp.sk_date_assessment_assigned,
			tmp.sk_date_assessment_finished,
			tmp.sk_date_attempt_start,
			tmp.sk_date_attempt_closed,
			COALESCE(dd.sk_dim_district, -1)       	AS sk_dim_district,
			COALESCE(ds.sk_dim_user, -1)            AS sk_dim_student,
			COALESCE(dt.sk_dim_user, -1)            AS sk_dim_teacher,
			COALESCE(dc.sk_dim_classroom, -1)     	AS sk_dim_classroom,
			COALESCE(dsy.sk_dim_school_year, -1) 	AS sk_dim_school_year,
			COALESCE(dg.sk_dim_grade, -1)           AS sk_dim_grade,
			COALESCE(dp.sk_dim_product, -1)         AS sk_dim_product,
			COALESCE(dcr.sk_dim_curriculum, -1)  	AS sk_dim_curriculum,
			COALESCE(dq.sk_dim_question, -1)       	AS sk_dim_question,
			COALESCE(drv.sk_dim_rating_variation, case when tmp.tmp_scoring_type ='rating' then -1 else -2 end )       	AS sk_dim_rating_variation,
			tmp.des_assessment_type,
			tmp.des_assessment_status,
			tmp.flg_assessment_auto_launch,
			tmp.flg_asessment_deleted,
			tmp.des_attempt_status,
			tmp.flg_attempt_latest,
			tmp.flg_attempt_deleted,
			tmp.flg_attempt_graded,
			tmp.flg_attempt_completed_on_paper,
			tmp.des_attempt_max_score,
            tmp.questions_rubric_item_key,
            tmp.questions_rubric_item_value,
            tmp.assessment_created_at,
			tmp.assessment_updated_at,
			tmp.attempt_created_at,
			tmp.attempt_updated_at,
			tmp.questions_rubric_created_at,
            tmp.questions_rubric_updated_at,
			'AFFIRM'									AS dwh_source_system_cd,
			'ASSESSMENTS,ATTEMPTS,ASSESSMENT_TEMPLATES,QUESTIONS_RUBRIC' AS dwh_source_data_cd
  FROM tmp_fct_questions_rubric AS tmp
         LEFT JOIN edw.dim_school_year 	AS dsy ON dsy.bk_dim_school_year = tmp.bk_dim_school_year
         LEFT JOIN edw.dim_district 	AS dd  ON dd.bk_dim_district = tmp.bk_dim_district
         								  AND tmp.tmp_date_assessment_start BETWEEN dd.dwh_date_start AND dd.dwh_date_end
         LEFT JOIN edw.dim_user 		AS ds ON ds.id_student_global = tmp.id_student_global
         								  AND tmp.tmp_date_assessment_start BETWEEN ds.dwh_date_start AND ds.dwh_date_end
                                          AND ds.flg_is_student
         								  AND ds.flg_last_version_id_student_global
         LEFT JOIN edw.dim_user 		AS dt ON dt.id_teacher_global = tmp.id_teacher_global
         								  AND tmp.tmp_date_assessment_start BETWEEN dt.dwh_date_start AND dt.dwh_date_end
         								  AND dt.flg_is_teacher
         								  AND dt.flg_last_version_id_teacher_global
         LEFT JOIN edw.dim_classroom 	AS dc ON dc.id_classroom_global = tmp.id_classroom_global
         								  AND tmp.tmp_date_assessment_start BETWEEN dc.dwh_date_start AND dc.dwh_date_end
         								  AND dc.flg_last_version_id_classroom_global
         LEFT JOIN edw.dim_grade AS dg  ON dg.bk_dim_grade = dc.id_grade
										  AND  tmp.tmp_date_assessment_start BETWEEN dg.dwh_date_start AND dg.dwh_date_end
		 LEFT JOIN edw.dim_product AS dp ON dp.id_product_global = tmp.id_product_global
         								  AND tmp.tmp_date_assessment_start BETWEEN dp.dwh_date_start AND dp.dwh_date_end
         								  AND dp.flg_last_version_id_product_global
         LEFT JOIN edw.dim_curriculum AS dcr ON dcr.id_curriculum_global = tmp.id_curriculum_global
         								  AND tmp.tmp_date_assessment_start BETWEEN dcr.dwh_date_start AND dcr.dwh_date_end
         								  AND dcr.flg_last_version_id_curriculum_global
         LEFT JOIN edw.dim_question AS dq ON dq.bk_dim_question = tmp.bk_dim_question AND
                                                       tmp.tmp_date_assessment_start BETWEEN dq.dwh_date_start AND dq.dwh_date_end
          LEFT JOIN edw.dim_rating_variation AS drv ON drv.bk_dim_rating_variation = tmp.bk_dim_rating_variation
