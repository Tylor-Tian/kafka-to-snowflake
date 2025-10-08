-- =========================================
-- 020_mart_views.sql
-- 解析视图 + 事实表 + （可选）增量加载 Stream/Task
-- =========================================
ALTER SESSION SET TIMEZONE='UTC';

-- 参数
SET DB_NAME   = 'DEMO_DW';
SET RAW_SCHEMA  = 'RAW';
SET MART_SCHEMA = 'MART';
SET MON_SCHEMA  = 'MART_MON';
SET WH_NAME   = 'WH_TINY';

USE ROLE MART_ROLE;
USE WAREHOUSE IDENTIFIER($WH_NAME);
USE DATABASE IDENTIFIER($DB_NAME);
USE SCHEMA   IDENTIFIER($MART_SCHEMA);

-- 解析视图（强韧性清洗）
-- 逻辑：
--  - 若 RECORD_CONTENT 已是 OBJECT，直接用；
--  - 否则把其当作字符串清洗：去结尾字面 `n、去外层引号、把 \" 还原为 "，再 TRY_PARSE_JSON。
CREATE OR REPLACE VIEW V_EVENTS_PARSED_RAW AS
WITH BASE AS (
  SELECT
    r.RECORD_METADATA:"CreateTime"::NUMBER/1000 AS create_s,
    r.RECORD_METADATA:"key"::STRING             AS event_key,
    r.RECORD_CONTENT                            AS content
  FROM IDENTIFIER($DB_NAME)||'.'||IDENTIFIER($RAW_SCHEMA)||'.EVENTS_RAW_B' r
),
CLEAN AS (
  SELECT
    create_s,
    event_key,
    IFF(TYPEOF(content)='OBJECT',
        TO_VARIANT(content),
        TRY_PARSE_JSON(
          REPLACE(                         -- 3) unescape \" → "
            REGEXP_REPLACE(                -- 2) trim outer quotes
              REGEXP_REPLACE(              -- 1) drop trailing literal `n
                content::STRING, '`n$', ''
              ), '^"|"$', ''
            ),
          '\\"','"'
          )
        )
    ) AS v
  FROM BASE
),
J AS (
  SELECT
    TO_TIMESTAMP_NTZ(create_s)                         AS event_time,
    RTRIM(event_key)                                   AS event_key,   -- 去尾空白
    v
  FROM CLEAN
  WHERE v IS NOT NULL
)
SELECT
  event_time,
  event_key,
  v:"source"::STRING     AS source,
  v:"ts"::STRING         AS ts_iso,
  v:"city"::STRING       AS city,
  v:"region"::STRING     AS region,
  v:"lat"::FLOAT         AS lat,
  v:"lon"::FLOAT         AS lon,
  v:"temp_c"::FLOAT      AS temp_c,
  v:"humidity"::INT      AS humidity,
  v:"wind_speed"::FLOAT  AS wind_speed
FROM J;

-- 事实表（结构化明细，用于下游分析/看板）
CREATE OR REPLACE TABLE IDENTIFIER($DB_NAME)||'.'||IDENTIFIER($MART_SCHEMA)||'.F_EVENTS_PARSED' (
  EVENT_TIME   TIMESTAMP_NTZ,
  EVENT_KEY    STRING,
  SOURCE       STRING,
  CITY         STRING,
  REGION       STRING,
  LAT          FLOAT,
  LON          FLOAT,
  TEMP_C       FLOAT,
  HUMIDITY     INT,
  WIND_SPEED   FLOAT,
  LOAD_TS      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 首次回填（近 24 小时；可按需扩大窗口或去掉 WHERE 做全量）
INSERT INTO F_EVENTS_PARSED (
  EVENT_TIME, EVENT_KEY, SOURCE, CITY, REGION, LAT, LON, TEMP_C, HUMIDITY, WIND_SPEED
)
SELECT
  EVENT_TIME, EVENT_KEY, SOURCE, CITY, REGION, LAT, LON, TEMP_C, HUMIDITY, WIND_SPEED
FROM V_EVENTS_PARSED_RAW
WHERE EVENT_TIME >= DATEADD('hour', -24, CURRENT_TIMESTAMP());

-- （可选）增量：基于 RAW_B 的 Stream + 每 5 分钟写入事实表的 Task
USE SCHEMA IDENTIFIER($MON_SCHEMA);

CREATE OR REPLACE STREAM S_EVENTS_RAW_B
  ON TABLE IDENTIFIER($DB_NAME)||'.'||IDENTIFIER($RAW_SCHEMA)||'.EVENTS_RAW_B'
  SHOW_INITIAL_ROWS = FALSE;

CREATE OR REPLACE TASK TASK_LOAD_EVENTS_PARSED_5MIN
  WAREHOUSE = IDENTIFIER($WH_NAME)
  SCHEDULE  = '5 MINUTE'
AS
INSERT INTO IDENTIFIER($DB_NAME)||'.'||IDENTIFIER($MART_SCHEMA)||'.F_EVENTS_PARSED' (
  EVENT_TIME, EVENT_KEY, SOURCE, CITY, REGION, LAT, LON, TEMP_C, HUMIDITY, WIND_SPEED
)
SELECT
  TO_TIMESTAMP_NTZ(s.RECORD_METADATA:"CreateTime"::NUMBER/1000) AS event_time,
  RTRIM(s.RECORD_METADATA:"key"::STRING)                        AS event_key,
  j:"source"::STRING, j:"city"::STRING, j:"region"::STRING,
  j:"lat"::FLOAT,  j:"lon"::FLOAT,
  j:"temp_c"::FLOAT, j:"humidity"::INT, j:"wind_speed"::FLOAT
FROM S_EVENTS_RAW_B s,
     LATERAL (
       SELECT IFF(TYPEOF(s.RECORD_CONTENT)='OBJECT',
                  TO_VARIANT(s.RECORD_CONTENT),
                  TRY_PARSE_JSON(
                    REPLACE(
                      REGEXP_REPLACE(REGEXP_REPLACE(s.RECORD_CONTENT::STRING,'`n$',''), '^"|"$', ''),
                      '\\"','"'
                    )
                  )
       ) AS j
     )
WHERE j IS NOT NULL
  AND METADATA$ACTION='INSERT';

-- 默认启用（可按需手动 RESUME）
ALTER TASK TASK_LOAD_EVENTS_PARSED_5MIN RESUME;

-- 快速验证
USE SCHEMA IDENTIFIER($MART_SCHEMA);
SELECT COUNT(*) AS rows_15m,
       MIN(EVENT_TIME) AS min_t,
       MAX(EVENT_TIME) AS max_t
FROM F_EVENTS_PARSED
WHERE EVENT_TIME >= DATEADD('minute', -15, CURRENT_TIMESTAMP());
