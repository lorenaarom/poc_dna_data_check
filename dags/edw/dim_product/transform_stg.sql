CREATE TEMP TABLE stg_dim_product AS
  SELECT p.id        AS bk_dim_product,
         p.id        AS id_product,
         p.global_id AS id_product_global,
         p.name      AS des_product,
         ROW_NUMBER() over (PARTITION by p.global_id order by  greatest(p.created_at  ,p.updated_at   ,p.deleted_at) desc  ) =1 AS flg_last_version_id_product_global,
         'ACMA'      AS dwh_source_system_cd,
         'PRODUCT'   AS dwh_source_data_cd
  FROM account_management.product AS p