-- ACCOUNTADMIN�Ŏ��s
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

-- ACCOUNT_USAGE�X�L�[�}�ւ̃A�N�Z�X�e�X�g
USE ROLE frosty_friday;
SELECT COUNT(*) FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES;

-- �ʏ�̃e�[�u���쐬
USE ROLE ACCOUNTADMIN;
USE DATABASE FROSTY_FRIDAY;
USE SCHEMA WEEK_066;

CREATE TABLE demo_normal_table (
    id INTEGER,
    name STRING,
    created_date DATE
);

-- �f�[�^�}���iROW_COUNT > 0 �ɂ��邽�߁j
INSERT INTO demo_normal_table VALUES 
(1, 'Sample Data 1', '2025-01-01'),
(2, 'Sample Data 2', '2025-01-02'),
(3, 'Sample Data 3', '2025-01-03');


-- �ꎞ�I�e�[�u���쐬�i����1�Ɉ���������j
CREATE TRANSIENT TABLE demo_transient_table (
    id INTEGER,
    name STRING,
    created_date DATE
);

-- �f�[�^�}��
INSERT INTO demo_transient_table VALUES 
(1, 'Transient Data 1', '2025-01-01'),
(2, 'Transient Data 2', '2025-01-02');


-- ��̃e�[�u���쐬�i����3�Ɉ���������j
CREATE TABLE demo_empty_table (
    id INTEGER,
    name STRING,
    created_date DATE
);

-- �f�[�^�͑}�����Ȃ��iROW_COUNT = 0�j


-- �폜�Ώۃe�[�u���쐬
CREATE TABLE demo_to_be_deleted (
    id INTEGER,
    name STRING,
    created_date DATE
);

-- �f�[�^�}��
INSERT INTO demo_to_be_deleted VALUES 
(1, 'Will be deleted', '2025-01-01');

-- �e�[�u���폜�i����2�Ɉ���������j
DROP TABLE demo_to_be_deleted;


-- ACCOUNT_USAGE����m�F�i���f�Ƀ��O������̂Œ��� 10�����炢�H�j
SELECT 
    TABLE_NAME,
    IS_TRANSIENT,
    DELETED,
    ROW_COUNT,
    CREATED
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES 
WHERE TABLE_NAME LIKE 'DEMO_%' 
    AND TABLE_CATALOG = 'FROSTY_FRIDAY'
    AND TABLE_SCHEMA = 'WEEK_066'  -- �C���ӏ�
ORDER BY TABLE_NAME;



-- ���݂̃f�[�^�x�[�X�̃e�[�u�����i�������͑����Ō�����j
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
