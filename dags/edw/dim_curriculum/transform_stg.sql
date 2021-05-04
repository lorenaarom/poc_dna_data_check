CREATE TEMP TABLE stg_dim_curriculum AS
  SELECT c.id         AS bk_dim_curriculum,
         c.id         AS id_curriculum,
         c.global_id  AS id_curriculum_global,
         c.name       AS des_curriculum,
         ROW_NUMBER() OVER (PARTITION BY c.global_id ORDER BY GREATEST(c.created_at, c.updated_at, c.deleted_at) DESC) =
         1            AS flg_last_version_id_curriculum_global,
         'ACMA'       AS dwh_source_system_cd,
         'CURRICULUM' AS dwh_source_data_cd
  FROM account_management.curriculum AS c

