CREATE TEMP TABLE stg_dim_classroom AS
  SELECT c.id                                 AS bk_dim_classroom,
         COALESCE(dsy.sk_dim_school_year, -1) AS sk_school_year_enrollment,
         COALESCE(du.sk_dim_user, -1)         AS sk_dim_teacher_primary,
         COALESCE(dd.sk_dim_district, -1)     AS sk_dim_district,
         s.id                                 AS id_school,
         s.external_id                        AS id_school_external,
         s.global_id                          AS id_school_global,
         s.name                               AS des_school,
         s.zip_code                           AS des_school_zip_code,
         g.id                                 AS id_grade,
         g.global_id                          AS id_grade_global,
         g."name"                             AS des_grade,
         c.external_grade                     AS des_grade_external,
         c.id                                 AS id_classroom,
         c.external_id                        AS id_classroom_external,
         c.global_id                          AS id_classroom_global,
         c.name                               AS des_classroom,
         c.external_subject                   AS des_classroom_external_subject,
         ROW_NUMBER() OVER (PARTITION by c.global_id
           ORDER BY GREATEST(c.created_at, c.updated_at, c.deleted_at) DESC) =
         1                                    AS flg_last_version_id_classroom_global,
         'ACMA'                               AS dwh_source_system_cd,
         'SCHOOL-CLASSROOM-GRADE'             AS dwh_source_data_cd
  FROM account_management.school AS s
         INNER JOIN account_management.classroom AS c ON s.id = c.school_id
         LEFT OUTER JOIN account_management.grade AS g ON g.id = c.grade_id
         LEFT OUTER JOIN account_management.teacher as t on c.primary_teacher_id = t.id
         LEFT OUTER JOIN edw.dim_school_year AS dsy ON dsy.bk_dim_school_year = c.enrollment_period_id
         LEFT OUTER JOIN edw.dim_district AS dd ON dd.bk_dim_district = s.district_id and dd.dwh_flg_current_version
         LEFT OUTER JOIN edw.dim_user AS du ON du.bk_dim_user = t.user_id ||'$'|| t.id and du.dwh_flg_current_version
                                                 and du.flg_is_teacher
