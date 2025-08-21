-- ACCOUNTADMINで実行
USE ROLE ACCOUNTADMIN;

CREATE  ROLE FROSTY_FRIDAY;
CREATE WAREHOUSE FROSTY_FRIDAY_XS;
CREATE DATABASE FROSTY_FRIDAY ;
CREATE SCHEMA FROSTY_FRIDAY.WEEK_066 ;

GRANT USAGE ON WAREHOUSE FROSTY_FRIDAY_XS TO ROLE FROSTY_FRIDAY;
GRANT USAGE ON WAREHOUSE FROSTY_FRIDAY_SP_S TO ROLE FROSTY_FRIDAY;
GRANT USAGE ON DATABASE FROSTY_FRIDAY TO ROLE FROSTY_FRIDAY;
GRANT USAGE ON SCHEMA FROSTY_FRIDAY.WEEK_066 TO ROLE FROSTY_FRIDAY;
GRANT SELECT ON ALL TABLES IN SCHEMA FROSTY_FRIDAY.WEEK_066 TO ROLE FROSTY_FRIDAY;

-- ACCOUNT_USAGEスキーマへのアクセステスト
USE ROLE frosty_friday;
SELECT COUNT(*) FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES;

-- 通常のテーブル作成
USE ROLE ACCOUNTADMIN;
USE DATABASE FROSTY_FRIDAY;
USE SCHEMA WEEK_066;

CREATE TABLE demo_normal_table (
    id INTEGER,
    name STRING,
    created_date DATE
);

-- データ挿入（ROW_COUNT > 0 にするため）
INSERT INTO demo_normal_table VALUES 
(1, 'Sample Data 1', '2025-01-01'),
(2, 'Sample Data 2', '2025-01-02'),
(3, 'Sample Data 3', '2025-01-03');


-- 一時的テーブル作成（条件1に引っかかる）
CREATE TRANSIENT TABLE demo_transient_table (
    id INTEGER,
    name STRING,
    created_date DATE
);

-- データ挿入
INSERT INTO demo_transient_table VALUES 
(1, 'Transient Data 1', '2025-01-01'),
(2, 'Transient Data 2', '2025-01-02');


-- 空のテーブル作成（条件3に引っかかる）
CREATE TABLE demo_empty_table (
    id INTEGER,
    name STRING,
    created_date DATE
);

-- データは挿入しない（ROW_COUNT = 0）


-- 削除対象テーブル作成
CREATE TABLE demo_to_be_deleted (
    id INTEGER,
    name STRING,
    created_date DATE
);

-- データ挿入
INSERT INTO demo_to_be_deleted VALUES 
(1, 'Will be deleted', '2025-01-01');

-- テーブル削除（条件2に引っかかる）
DROP TABLE demo_to_be_deleted;


-- ACCOUNT_USAGEから確認（反映にラグがあるので注意 10分ぐらい？）
SELECT 
    TABLE_NAME,
    IS_TRANSIENT,
    DELETED,
    ROW_COUNT,
    CREATED
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES 
WHERE TABLE_NAME LIKE 'DEMO_%' 
    AND TABLE_CATALOG = 'FROSTY_FRIDAY'
    AND TABLE_SCHEMA = 'WEEK_066'  -- 修正箇所
ORDER BY TABLE_NAME;



-- 現在のデータベースのテーブル情報（こっちは即時で見える）
USE DATABASE FROSTY_FRIDAY;

SELECT 
    TABLE_CATALOG,
    TABLE_SCHEMA, 
    TABLE_NAME,
    TABLE_TYPE,
    IS_TRANSIENT,
    ROW_COUNT,
    BYTES,
    CREATED,
    LAST_ALTERED
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'WEEK_066'
ORDER BY TABLE_NAME;
