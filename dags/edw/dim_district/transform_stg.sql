CREATE TEMP TABLE stg_dim_district AS
  SELECT d.id                                 AS bk_dim_district,
         COALESCE(dsy.sk_dim_school_year, -1) AS sk_dim_school_year,
         d.id                                 AS id_district,
         d.external_id                        AS id_district_external,
         d.state                              AS id_state,
         d.name                               AS des_district,
         d.contact_name                       AS des_contact_name,
         d.contact_type                       AS des_contact_type,
         d.contact_email                      AS des_contact_email,
         d.roster_strategy                    AS des_roster_strategy,
         'ACMA'                               AS dwh_source_system_cd,
         'DISTRICT'                           AS dwh_source_data_cd
  FROM account_management.district d
         LEFT OUTER JOIN edw.dim_school_year dsy ON d.active_period_id = dsy.bk_dim_school_year