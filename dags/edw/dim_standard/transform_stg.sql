CREATE TEMP TABLE stg_dim_standard AS
  SELECT si.ID                                                                                   AS bk_dim_standard,
         ss.ID                                                                                   AS id_standard_set,
         ss.global_id                                                                            AS id_standard_set_global,
         ss.provider_id                                                                          AS id_standard_set_provider,
         ss.name                                                                                 AS des_standard_set,
         si.subject                                                                              AS des_standard_set_subject,
         si.id                                                                                   AS id_standard_item,
         si.global_id                                                                            AS id_standard_item_global,
         si.ancestors_global_id                                                                  AS id_standard_item_global_parent,
         si.hierarchy_level_order                                                                AS id_standard_item_hierarchy_level_order,
         si.hierarchy_level                                                                      AS des_standard_item_hierarchy_level,
         si.code                                                                                 AS des_standard_item_code,
         si.short_code                                                                           AS des_standard_item_short_code,
         si.description                                                                          AS des_standard_item,
         ROW_NUMBER() OVER (PARTITION BY si.global_id ORDER BY GREATEST(si.created_at, si.updated_at,
                                                                        si.deleted_at) DESC) =
         1                                                                                       AS flg_last_version_id_standard_item_global,
         'STANDARDS'                                                                             AS dwh_source_system_cd,
         'STANDARD_SETS,STANDARD_ITEMS'                                                          AS dwh_source_data_cd
  FROM standards.standard_sets AS ss
         INNER JOIN standards.standard_items si ON ss.id = si.standard_set_id