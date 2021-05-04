CREATE TEMP TABLE stg_dim_time AS
  WITH tmp_range AS (
      SELECT to_date('{{ ds }}', 'YYYY-MM-DD', TRUE) -
             ((ROW_NUMBER() OVER (ORDER BY recordtime DESC) - 365) * INTERVAL '1 DAYS') AS id_date
      FROM stl_connection_log
      ORDER BY recordtime DESC
      LIMIT 365 * 2
  )
  SELECT
       /* 00 */ DATE_PART('doy', id_date)                                AS id_day_of_year,           -- OK
       /* 01 */ DATE_PART('day', id_date)                                AS id_day_of_month,          -- OK
       /* 02 */ CASE DATE_PART('dow', id_date)
                  WHEN 0 THEN 7
                  ELSE DATE_PART('dow', id_date) END                     AS id_day_of_week,           -- OK
       /* 06 */ CASE
                  WHEN id_date = LAST_DAY(id_date) AND MOD(DATE_PART('month', id_date), 6) = 0
                      THEN true
                  ELSE false END                                         AS flg_last_day_of_semester, -- OK
       /* 03 */ CASE
                  WHEN id_date = LAST_DAY(id_date) AND MOD(DATE_PART('month', id_date), 3) = 0
                      THEN true
                  ELSE false END                                         AS flg_last_day_of_quarter,  -- OK
       /* 04 */ id_date = LAST_DAY(id_date)                              AS flg_last_day_of_month,    -- OK
       /* 05 */ DATE_PART('dow', id_date) = 0                            AS flg_last_day_of_week,     -- OK
       /* 07 */ DATE_PART('w', id_date)                                  AS id_week,                  -- OK
       /* 08 */ DATE_PART('mon', id_date)                                AS id_month,                 -- OK
       /* 09 */ DATE_PART('y', id_date)                                  AS id_year,                  -- OK
       /* 10 */ DATE_PART('y', id_date) * 10000 + DATE_PART('mon', id_date) * 100 +
                DATE_PART('day', id_date)                                AS sk_dim_time,              -- OK
       /* 11 */ 'EDW'                                                    AS dwh_source_data_cd,       -- OK
       /* 12 */ 'PROCEDURE'                                              AS dwh_source_system_cd,     -- OK
       /* 13 */ CONCAT('Week ', DATE_PART('w', id_date) :: varchar(16))  AS des_week,                 -- OK
       /* 14 */ to_char(id_date, 'Month')                                AS des_month,                -- OK
       /* 15 */ CONCAT('Quarter ', to_char(id_date, 'Q') :: varchar(16)) AS des_quarter,              -- OK
       /* 16 */ DATE_PART('qtr', id_date)                                AS id_quarter,               -- OK
       /* 17 */ CASE
                  WHEN DATE_PART('mon', id_date) BETWEEN 1 AND 6 THEN 'Semester 1'
                  WHEN DATE_PART('mon', id_date) BETWEEN 7 AND 12 THEN 'Semester 2'
                    END                                                  AS des_semester,             -- OK
       /* 18 */ CASE
                  WHEN DATE_PART('mon', id_date) BETWEEN 1 AND 6 THEN '1'
                  WHEN DATE_PART('mon', id_date) BETWEEN 7 AND 12 THEN '2'
                    END                                                  AS id_semester,              -- OK
       /* 19 */ to_char(id_date, 'YYYY')                                 AS des_year,                 -- OK
       /* 20 */ to_char(id_date, 'YYYY-MM-DD')                           AS bk_dim_time,              -- OK
       /* 21 */ id_date :: DATE                                          AS id_date                   -- OK
FROM tmp_range
