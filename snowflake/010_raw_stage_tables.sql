-- =========================================
-- 010_raw_stage_tables.sql
-- RAW 表（Kafka Sink 落地）
-- =========================================
ALTER SESSION SET TIMEZONE='UTC';

-- 参数
SET DB_NAME   = 'DEMO_DW';
SET RAW_SCHEMA  = 'RAW';
SET WH_NAME   = 'WH_TINY';

USE ROLE MART_ROLE;
USE WAREHOUSE IDENTIFIER($WH_NAME);
USE DATABASE IDENTIFIER($DB_NAME);
USE SCHEMA   IDENTIFIER($RAW_SCHEMA);

-- Kafka → Snowflake Sink 目标表（通用）
-- Sink Connector 的 "snowflake.topic2table.map": "events:EVENTS_RAW_B"
CREATE TABLE IF NOT EXISTS EVENTS_RAW_B (
  RECORD_METADATA OBJECT,   -- 包含 key/partition/offset/CreateTime 等
  RECORD_CONTENT  VARIANT   -- 消息体（可为 VARIANT 或 JSON-string）
);

-- （可选）辅助视图：把常用元数据展开，便于直接查询
CREATE OR REPLACE VIEW V_EVENTS_RAW_B_META AS
SELECT
  TO_TIMESTAMP_NTZ(RECORD_METADATA:"CreateTime"::NUMBER/1000) AS CREATE_TS,
  RECORD_METADATA:"key"::STRING                               AS EVENT_KEY,
  RECORD_METADATA                                             AS META,
  RECORD_CONTENT                                              AS CONTENT
FROM EVENTS_RAW_B;

-- 验证（无数据也可运行）
SELECT * FROM V_EVENTS_RAW_B_META ORDER BY CREATE_TS DESC LIMIT 10;
