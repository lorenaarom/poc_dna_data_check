CREATE TEMP TABLE stg_dim_school_year AS
  SELECT id                  AS bk_dim_school_year,
         id                  AS id_school_year,
         external_id         AS id_school_year_external,
         name                AS des_school_year,
         start_at            AS id_start_at,
         end_at              AS id_end_at,
         'ACMA'              AS dwh_source_system_cd,
         'ENROLLMENT_PERIOD' AS dwh_source_data_cd
  FROM account_management.enrollment_period