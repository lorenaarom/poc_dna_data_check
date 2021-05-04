CREATE TEMP TABLE stg_dim_geography AS
SELECT cast(id_country as varchar) || '-' || cast(id_state as varchar) || '-' || id_city AS bk_dim_geography,
    id_country,
    des_country,
    id_state,
    des_state,
    id_city,
    des_city,
    'EDW' AS dwh_source_system_cd,
    'PAR_GEOGRAPHY' AS dwh_source_data_cd
FROM edw.par_geography;