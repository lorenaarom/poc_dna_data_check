CREATE TEMP TABLE stg_dim_rating_variation  AS
  select variation || '$' || category_order AS bk_dim_rating_variation,
         variation                          AS id_variation,
         'Variation ' || variation          AS des_variation,
         category_order                     AS id_category_order,
         category_name                      AS des_category,
         'DWH'                              AS dwh_source_system_cd,
         'RATING_VARIATION'                 AS dwh_source_data_cd
  from assessments.rating_variations
