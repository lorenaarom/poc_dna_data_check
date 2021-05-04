CREATE TEMP TABLE stg_dim_grade AS
  SELECT g.id        AS bk_dim_grade,
         g.id        AS id_grade,
         g.global_id AS id_grade_global,
         g.name      AS des_grade,
         ROW_NUMBER() OVER (PARTITION by g.global_id ORDER BY GREATEST(g.created_at, g.updated_at, g.deleted_at) DESC) =
         1           AS flg_last_version_id_grade_global,
         'ACMA'      AS dwh_source_system_cd,
         'GRADE'     AS dwh_source_data_cd
  FROM account_management.grade AS g