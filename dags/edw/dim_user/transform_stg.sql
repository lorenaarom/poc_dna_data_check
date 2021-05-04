CREATE TEMP TABLE stg_dim_user AS
  WITH tmp_base AS
  (
      SELECT COALESCE(dd.sk_dim_district, -1)                               AS sk_dim_district,
             COALESCE(dsy.sk_dim_school_year, -1)                           AS sk_school_year_enrollment,
             u.id                                                           AS id_user,
             u.username                                                     AS id_user_name,
             u.cognito_sub                                                  AS id_user_cognito_sub,
             u.email                                                        AS des_user_email,
             u.first_name                                                   AS des_user_first_name,
             u.middle_name                                                  AS des_user_middle_name,
             u.last_name                                                    AS des_user_last_name,
             u.gender                                                       AS des_user_gender,
             u.date_of_birth                                                AS des_user_date_of_birth,
             u.created_at                                                   AS created_at,
             u.updated_at                                                   AS updated_at,
             u.deleted_at                                                   AS deleted_at,
             ROW_NUMBER() over (PARTITION by u.username order by
               greatest(u.created_at, u.updated_at, u.deleted_at) desc) = 1 AS flg_last_version_id_user_name,
             ROW_NUMBER() over (PARTITION by u.cognito_sub order by
               greatest(u.created_at, u.updated_at, u.deleted_at) desc) = 1 AS flg_last_version_id_user_cognito_sub,
             'ACMA'                                                         AS dwh_source_system_cd,
             'USER,STUDENT,TEACHER,SCHOOL_ADMIN,DISTRICT_ADMIN'             AS dwh_source_data_cd
      FROM account_management.user AS u
             LEFT OUTER JOIN edw.dim_school_year AS dsy ON dsy.bk_dim_school_year = u.enrollment_period_id
             LEFT OUTER JOIN edw.dim_district AS dd ON dd.bk_dim_district = u.district_id and dd.dwh_flg_current_version
  )
  --student data
  SELECT u.id_user ||'$'|| s.id                                  AS bk_dim_user,
         u.sk_dim_district,
         u.sk_school_year_enrollment,
         u.id_user,
         u.id_user_name,
         u.id_user_cognito_sub,
         u.des_user_email,
         u.des_user_first_name,
         u.des_user_middle_name,
         u.des_user_last_name,
         u.des_user_gender,
         u.des_user_date_of_birth,
         u.flg_last_version_id_user_name,
         u.flg_last_version_id_user_cognito_sub,
      --student data
         TRUE                                                    AS flg_is_student,
         s.id                                                    AS id_student,
         s.external_id                                           AS id_student_external,
         s.global_id                                             AS id_student_global,
         s.sis_id                                                AS id_student_ssis,
         s.student_number                                        AS des_student_number,
         s.graduation_year                                       AS des_student_graduation_year,
         s.limited_english                                       AS flg_student_limited_english,
         s.race                                                  AS des_student_race,
         s.home_language                                         AS des_student_home_language,
         ROW_NUMBER() over (PARTITION by s.global_id order by
           greatest(u.created_at, u.updated_at, u.deleted_at
           , s.created_at, s.updated_at, s.deleted_at) desc) = 1 AS flg_last_version_id_student_global,
      --teacher data
         FALSE                                                   AS flg_is_teacher,
         NULL                                                    AS id_teacher,
         NULL                                                    AS id_teacher_global,
         NULL                                                    AS id_teacher_external,
         NULL                                                    AS des_teacher_number,
         FALSE                                                   AS flg_last_version_id_teacher_global,
      --school_admin data
         FALSE                                                   AS flg_is_school_admin,
         NULL                                                    AS id_school_admin,
         NULL                                                    AS id_school_admin_global,
         NULL                                                    AS id_school_admin_external,
         NULL                                                    AS des_school_admin_number,
         FALSE                                                   AS flg_last_version_id_school_admin_global,
      --district_admin data
         FALSE                                                   AS flg_is_district_admin,
         NULL                                                    AS id_district_admin,
         NULL                                                    AS id_district_admin_global,
         NULL                                                    AS id_district_admin_external,
         NULL                                                    AS des_district_admin_number,
         FALSE                                                   AS flg_last_version_id_district_admin_global,
         u.dwh_source_system_cd,
         u.dwh_source_data_cd
  FROM tmp_base AS u
         INNER JOIN account_management.student AS s ON s.user_id = u.id_user
  UNION ALL
  --teacher data
  SELECT u.id_user ||'$'|| t.id                                  AS bk_dim_user,
         u.sk_dim_district,
         u.sk_school_year_enrollment,
         u.id_user,
         u.id_user_name,
         u.id_user_cognito_sub,
         u.des_user_email,
         u.des_user_first_name,
         u.des_user_middle_name,
         u.des_user_last_name,
         u.des_user_gender,
         u.des_user_date_of_birth,
         u.flg_last_version_id_user_name,
         u.flg_last_version_id_user_cognito_sub,
      --student data
         FALSE                                                   AS flg_is_student,
         NULL                                                    AS id_student,
         NULL                                                    AS id_student_external,
         NULL                                                    AS id_student_global,
         NULL                                                    AS id_student_ssis,
         NULL                                                    AS des_student_number,
         NULL                                                    AS des_student_graduation_year,
         NULL                                                    AS flg_student_limited_english,
         NULL                                                    AS des_student_race,
         NULL                                                    AS des_student_home_language,
         FALSE                                                   AS flg_last_version_id_student_global,
      --teacher data
         TRUE                                                    AS flg_is_teacher,
         t.id                                                    AS id_teacher,
         t.global_id                                             AS id_teacher_global,
         t.external_id                                           AS id_teacher_external,
         t.teacher_number                                        AS des_teacher_number,
         ROW_NUMBER() over (PARTITION by t.global_id order by
           greatest(u.created_at, u.updated_at, u.deleted_at
           , t.created_at, t.updated_at, t.deleted_at) desc) = 1 AS flg_last_version_id_teacher_global,
      --school_admin data
         FALSE                                                   AS flg_is_school_admin,
         NULL                                                    AS id_school_admin,
         NULL                                                    AS id_school_admin_global,
         NULL                                                    AS id_school_admin_external,
         NULL                                                    AS des_school_admin_number,
         FALSE                                                   AS flg_last_version_id_school_admin_global,
      --district_admin data
         FALSE                                                   AS flg_is_district_admin,
         NULL                                                    AS id_district_admin,
         NULL                                                    AS id_district_admin_global,
         NULL                                                    AS id_district_admin_external,
         NULL                                                    AS des_district_admin_number,
         FALSE                                                   AS flg_last_version_id_district_admin_global,
         u.dwh_source_system_cd,
         u.dwh_source_data_cd
  FROM tmp_base AS u
         INNER JOIN account_management.teacher AS t ON t.user_id = u.id_user
  UNION ALL
  --school_admin data
  SELECT u.id_user ||'$'|| sa.id                                    AS bk_dim_user,
         u.sk_dim_district,
         u.sk_school_year_enrollment,
         u.id_user,
         u.id_user_name,
         u.id_user_cognito_sub,
         u.des_user_email,
         u.des_user_first_name,
         u.des_user_middle_name,
         u.des_user_last_name,
         u.des_user_gender,
         u.des_user_date_of_birth,
         u.flg_last_version_id_user_name,
         u.flg_last_version_id_user_cognito_sub,
      --student data
         FALSE                                                      AS flg_is_student,
         NULL                                                       AS id_student,
         NULL                                                       AS id_student_external,
         NULL                                                       AS id_student_global,
         NULL                                                       AS id_student_ssis,
         NULL                                                       AS des_student_number,
         NULL                                                       AS des_student_graduation_year,
         NULL                                                       AS flg_student_limited_english,
         NULL                                                       AS des_student_race,
         NULL                                                       AS des_student_home_language,
         FALSE                                                      AS flg_last_version_id_student_global,
      --teacher data
         FALSE                                                      AS flg_is_teacher,
         NULL                                                       AS id_teacher,
         NULL                                                       AS id_teacher_global,
         NULL                                                       AS id_teacher_external,
         NULL                                                       AS des_teacher_number,
         FALSE                                                      AS flg_last_version_id_teacher_global,
      --school_admin data
         TRUE                                                       AS flg_is_school_admin,
         sa.id                                                      AS id_school_admin,
         sa.global_id                                               AS id_school_admin_global,
         sa.external_id                                             AS id_school_admin_external,
         sa.admin_number                                            AS des_school_admin_number,
         ROW_NUMBER() OVER (PARTITION BY sa.global_id ORDER BY
           GREATEST(u.created_at, u.updated_at, u.deleted_at
           , sa.created_at, sa.updated_at, sa.deleted_at) DESC) = 1 AS flg_last_version_id_school_admin_global,
      --district_admin data
         FALSE                                                      AS flg_is_district_admin,
         NULL                                                       AS id_district_admin,
         NULL                                                       AS id_district_admin_global,
         NULL                                                       AS id_district_admin_external,
         NULL                                                       AS des_district_admin_number,
         FALSE                                                      AS flg_last_version_id_district_admin_global,
         u.dwh_source_system_cd,
         u.dwh_source_data_cd
  FROM tmp_base AS u
         INNER JOIN account_management.school_admin AS sa ON sa.user_id = u.id_user
  UNION ALL
  --district_admin data
  SELECT u.id_user ||'$'|| da.id                                    AS bk_dim_user,
         u.sk_dim_district,
         u.sk_school_year_enrollment,
         u.id_user,
         u.id_user_name,
         u.id_user_cognito_sub,
         u.des_user_email,
         u.des_user_first_name,
         u.des_user_middle_name,
         u.des_user_last_name,
         u.des_user_gender,
         u.des_user_date_of_birth,
         u.flg_last_version_id_user_name,
         u.flg_last_version_id_user_cognito_sub,
      --student data
         FALSE                                                      AS flg_is_student,
         NULL                                                       AS id_student,
         NULL                                                       AS id_student_external,
         NULL                                                       AS id_student_global,
         NULL                                                       AS id_student_ssis,
         NULL                                                       AS des_student_number,
         NULL                                                       AS des_student_graduation_year,
         NULL                                                       AS flg_student_limited_english,
         NULL                                                       AS des_student_race,
         NULL                                                       AS des_student_home_language,
         FALSE                                                      AS flg_last_version_id_student_global,
      --teacher data
         FALSE                                                      AS flg_is_teacher,
         NULL                                                       AS id_teacher,
         NULL                                                       AS id_teacher_global,
         NULL                                                       AS id_teacher_external,
         NULL                                                       AS des_teacher_number,
         FALSE                                                      AS flg_last_version_id_teacher_global,
      --school_admin data
         FALSE                                                      AS flg_is_school_admin,
         NULL                                                       AS id_school_admin,
         NULL                                                       AS id_school_admin_global,
         NULL                                                       AS id_school_admin_external,
         NULL                                                       AS des_school_admin_number,
         FALSE                                                      AS flg_last_version_id_school_admin_global,
      --district_admin data
         TRUE                                                       AS flg_is_district_admin,
         da.id                                                      AS id_district_admin,
         da.global_id                                               AS id_district_admin_global,
         da.external_id                                             AS id_district_admin_external,
         da.admin_number                                            AS des_district_admin_number,
         ROW_NUMBER() OVER (PARTITION BY da.global_id ORDER BY
           GREATEST(u.created_at, u.updated_at, u.deleted_at
           , da.created_at, da.updated_at, da.deleted_at) DESC) = 1 AS flg_last_version_id_district_admin_global,
         u.dwh_source_system_cd,
         u.dwh_source_data_cd
  FROM tmp_base AS u
         INNER JOIN account_management.district_admin AS da ON da.user_id = u.id_user
