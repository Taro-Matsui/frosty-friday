-- ========================================
-- Step 1：基本的なタスクDAGの実装
-- ========================================
-- 【目的】
-- Snowflake Tasksの基本的な使い方を学びます：
-- - ルートタスクと子タスクの関係
-- - 並列実行の仕組み
-- - データ投入から集約までの流れ
-- 
-- まずは基本的な実装を体験してみましょう。
-- ========================================
USE ROLE ACCOUNTADMIN;
USE DATABASE FROSTY_FRIDAY;
USE SCHEMA WEEK_081;
USE WAREHOUSE week81_wh;

-- ----------------------------------------
-- 環境クリア
-- ----------------------------------------

ALTER TASK IF EXISTS INSERT_INTO_RAW SUSPEND;
ALTER TASK IF EXISTS INSERT_INTO_CUSTOMERS SUSPEND;
ALTER TASK IF EXISTS INSERT_INTO_PRODUCT SUSPEND;
ALTER TASK IF EXISTS INSERT_INTO_SALES SUSPEND;
ALTER TASK IF EXISTS VALIDATE_DATA SUSPEND;
ALTER TASK IF EXISTS AGGREGATE_SALES SUSPEND;

DROP TASK IF EXISTS INSERT_INTO_RAW;
DROP TASK IF EXISTS INSERT_INTO_CUSTOMERS;
DROP TASK IF EXISTS INSERT_INTO_PRODUCT;
DROP TASK IF EXISTS INSERT_INTO_SALES;
DROP TASK IF EXISTS VALIDATE_DATA;
DROP TASK IF EXISTS AGGREGATE_SALES;

DROP VIEW IF EXISTS aggregated_sales;
DROP TABLE IF EXISTS data_validation_log;

DROP TABLE IF EXISTS w81_raw_product;
DROP TABLE IF EXISTS w81_raw_customer;
DROP TABLE IF EXISTS w81_raw_sales;

-- ----------------------------------------
-- テーブル作成（初期データなし）
-- ----------------------------------------
-- 注：Week 81の公式starter_codeが外部アクセス制限により実行できないため、
--     テーブル定義のみを手動で作成しています。
-- ----------------------------------------

CREATE TABLE w81_raw_product (
  data VARIANT,
  ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE w81_raw_customer (
  data VARIANT,
  ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE w81_raw_sales (
  data VARIANT,
  ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ----------------------------------------
-- タスク作成（公式解答：INSERT使用）
-- ----------------------------------------

-- Task 1: ルートタスク
CREATE OR REPLACE TASK INSERT_INTO_RAW
  WAREHOUSE = week81_wh
AS
  SELECT 'Raw data processing initiated' as status;

-- ----------------------------------------
-- Task 2～4: データ投入（並列実行）
-- ----------------------------------------
-- この3つのタスクは、すべてルートタスク（INSERT_INTO_RAW）の
-- 完了後に実行されます。
-- 
-- AFTER INSERT_INTO_RAW を指定しているため、
-- 3つのタスクは並列に実行されます。
-- ----------------------------------------

-- Task 2: 顧客データ投入
CREATE OR REPLACE TASK INSERT_INTO_CUSTOMERS
  WAREHOUSE = week81_wh
  AFTER INSERT_INTO_RAW
AS
  INSERT INTO w81_raw_customer (data)
  SELECT parse_json(column1) FROM VALUES
  ('{"customer_id": 6, "customer_name": "Frank", "email": "frank@example.com", "created_at": "2024-02-16"}'),
  ('{"customer_id": 7, "customer_name": "Grace", "email": "grace@example.com", "created_at": "2024-02-16"}');

-- Task 3: 製品データ投入
CREATE OR REPLACE TASK INSERT_INTO_PRODUCT
  WAREHOUSE = week81_wh
  AFTER INSERT_INTO_RAW
AS
  INSERT INTO w81_raw_product (data)
  SELECT parse_json(column1) FROM VALUES
  ('{"product_id": 21, "product_name": "Product U", "category": "Electronics", "price": 120.99, "created_at": "2024-02-16"}'),
  ('{"product_id": 22, "product_name": "Product V", "category": "Books", "price": 35.00, "created_at": "2024-02-16"}');

-- Task 4: 売上データ投入
CREATE OR REPLACE TASK INSERT_INTO_SALES
  WAREHOUSE = week81_wh
  AFTER INSERT_INTO_RAW
AS
  INSERT INTO w81_raw_sales (data)
  SELECT parse_json(column1) FROM VALUES
  ('{"sale_id": 11, "product_id": 21, "customer_id": 6, "quantity": 1, "sale_date": "2024-02-17"}'),
  ('{"sale_id": 12, "product_id": 22, "customer_id": 7, "quantity": 1, "sale_date": "2024-02-17"}'),
  ('{"sale_id": 13, "product_id": 21, "customer_id": 6, "quantity": 2, "sale_date": "2024-02-17"}'),
  ('{"sale_id": 14, "product_id": 22, "customer_id": 7, "quantity": 1, "sale_date": "2024-02-17"}'),
  ('{"sale_id": 15, "product_id": 21, "customer_id": 6, "quantity": 1, "sale_date": "2024-02-17"}');

-- Task 5: 集約処理
CREATE OR REPLACE TASK AGGREGATE_SALES
  WAREHOUSE = week81_wh
  AFTER INSERT_INTO_CUSTOMERS
AS
  CREATE OR REPLACE VIEW aggregated_sales AS
  WITH deduplicated_sales AS (
    SELECT DISTINCT
      data:sale_id::NUMBER as sale_id,
      data:product_id::NUMBER as product_id,
      data:customer_id::NUMBER as customer_id,
      data:quantity::NUMBER as quantity
    FROM w81_raw_sales
  ),
  deduplicated_customers AS (
    SELECT DISTINCT
      data:customer_id::NUMBER as customer_id,
      data:customer_name::VARCHAR as customer_name
    FROM w81_raw_customer
  ),
  deduplicated_products AS (
    SELECT DISTINCT
      data:product_id::NUMBER as product_id,
      data:product_name::VARCHAR as product_name,
      data:price::DECIMAL(10,2) as price
    FROM w81_raw_product
  )  
  SELECT 
    c.customer_name,
    p.product_name,
    SUM(s.quantity) as total_quantity,
    ROUND(SUM(s.quantity * p.price), 2) as total_revenue
  FROM deduplicated_sales s
  INNER JOIN deduplicated_customers c ON s.customer_id = c.customer_id
  INNER JOIN deduplicated_products p ON s.product_id = p.product_id
  GROUP BY c.customer_name, p.product_name;

-- ----------------------------------------
-- タスク有効化と実行
-- ----------------------------------------

ALTER TASK AGGREGATE_SALES RESUME;
ALTER TASK INSERT_INTO_SALES RESUME;
ALTER TASK INSERT_INTO_PRODUCT RESUME;
ALTER TASK INSERT_INTO_CUSTOMERS RESUME;

-- 【実行1回目】
EXECUTE TASK INSERT_INTO_RAW;
SELECT SYSTEM$WAIT(5, 'SECONDS');

-- ----------------------------------------
-- 結果確認
-- ----------------------------------------

-- タスクDAG構造確認
SELECT 
  t.name,
  COALESCE(ARRAY_SIZE(t.predecessors), 0) as parent_count,
  t.predecessors
FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS(
  task_name => 'INSERT_INTO_RAW', 
  recursive => TRUE
)) t
ORDER BY parent_count, t.name;

-- 最終結果
SELECT * FROM aggregated_sales
ORDER BY customer_name, product_name;

-- データ件数確認（1回目）
SELECT 
  'Step 1 - 1回目実行後' as execution,
  (SELECT COUNT(*) FROM w81_raw_customer WHERE data:customer_id::NUMBER IN (6,7)) as new_customers,
  (SELECT COUNT(*) FROM w81_raw_product WHERE data:product_id::NUMBER IN (21,22)) as new_products,
  (SELECT COUNT(*) FROM w81_raw_sales WHERE data:sale_id::NUMBER BETWEEN 11 AND 15) as new_sales,
  (SELECT COUNT(*) FROM aggregated_sales) as aggregated_records;

-- 期待結果：customers=2, products=2, sales=5, aggregated=2

-- ========================================
-- データの振る舞いを確認してみましょう
-- ========================================
-- 
-- 実務では、タスクが失敗してリトライする場合や、
-- 手動で再実行する場合があります。
-- 
-- では、もう一度同じタスクを実行してみましょう。
-- データがどのように変化するか確認してください。
-- ========================================

EXECUTE TASK INSERT_INTO_RAW;
SELECT SYSTEM$WAIT(5, 'SECONDS');

-- データ件数確認（2回目）
SELECT 
  'Step 1 - 2回目実行後' as execution,
  (SELECT COUNT(*) FROM w81_raw_customer WHERE data:customer_id::NUMBER IN (6,7)) as new_customers,
  (SELECT COUNT(*) FROM w81_raw_product WHERE data:product_id::NUMBER IN (21,22)) as new_products,
  (SELECT COUNT(*) FROM w81_raw_sales WHERE data:sale_id::NUMBER BETWEEN 11 AND 15) as new_sales,
  (SELECT COUNT(*) FROM aggregated_sales) as aggregated_records;

-- ========================================
-- 【確認】データの件数が変化しました
-- ========================================
-- customers: 2件 → 4件（倍増）
-- products: 2件 → 4件（倍増）
-- sales: 5件 → 10件（倍増）
-- aggregated_records: 2件（変わらず）
-- 
-- 【気になる点】：
-- - 生データテーブル（w81_raw_*）で件数が増加している
-- - INSERT文により、同じデータが再度追加されている
-- - 集約結果（aggregated_sales）は変わらない
--   → DISTINCTで重複を除外しているため
-- 
-- 【考えてみましょう】：
-- - このまま何度も実行したら、データはどうなるでしょうか？
-- - 実務で問題になる可能性はありますか？
-- 
-- → Step 2で、この振る舞いを改善する方法を学びます
-- ========================================
SELECT '★ Step 1完了：INSERTによる重複データ発生を体験' as lesson;


-- ========================================
-- Step 1 完了
-- ========================================
-- 【学んだこと】：
-- ✓ タスクDAGの基本構造（親子関係、並列実行）
-- ✓ データ投入から集約までの一連の流れ
-- 
-- 【次のStep】：
-- Step 2では、実行のたびにデータが増えていく振る舞いを
-- 改善する方法を学びます。
-- ========================================


-- ========================================
-- Step 2：データ投入方式の改善
-- ========================================
-- 【Step 1で気づいたこと】
-- タスクを再実行すると、生データテーブルで
-- 同じデータが重複して追加されていました。
-- 
-- 【このStepの目的】
-- 何度実行しても同じ結果になるように改善します。
-- 
-- 【改善のポイント】
-- 1. INSERT → MERGE
--    - 既存データとマッチングして、新規のみ追加
-- 
-- 2. 単一親タスク → 複数親タスク
--    - すべてのデータが揃ってから次の処理を実行
-- 
-- この2つの改善により、より堅牢なタスクDAGを構築します。
-- ========================================

USE ROLE ACCOUNTADMIN;
USE DATABASE FROSTY_FRIDAY;
USE SCHEMA WEEK_081;
USE WAREHOUSE week81_wh;

-- ----------------------------------------
-- 環境クリア
-- ----------------------------------------

ALTER TASK IF EXISTS INSERT_INTO_RAW SUSPEND;
ALTER TASK IF EXISTS INSERT_INTO_CUSTOMERS SUSPEND;
ALTER TASK IF EXISTS INSERT_INTO_PRODUCT SUSPEND;
ALTER TASK IF EXISTS INSERT_INTO_SALES SUSPEND;
ALTER TASK IF EXISTS VALIDATE_DATA SUSPEND;
ALTER TASK IF EXISTS AGGREGATE_SALES SUSPEND;

DROP TASK IF EXISTS INSERT_INTO_RAW;
DROP TASK IF EXISTS INSERT_INTO_CUSTOMERS;
DROP TASK IF EXISTS INSERT_INTO_PRODUCT;
DROP TASK IF EXISTS INSERT_INTO_SALES;
DROP TASK IF EXISTS VALIDATE_DATA;
DROP TASK IF EXISTS AGGREGATE_SALES;

DROP VIEW IF EXISTS aggregated_sales;
DROP TABLE IF EXISTS data_validation_log;

TRUNCATE TABLE IF EXISTS w81_raw_product;
TRUNCATE TABLE IF EXISTS w81_raw_customer;
TRUNCATE TABLE IF EXISTS w81_raw_sales;

-- ----------------------------------------
-- Task 1: ルートタスク
-- ----------------------------------------

CREATE OR REPLACE TASK INSERT_INTO_RAW
  WAREHOUSE = week81_wh
AS
  SELECT 'Raw data processing initiated' as status;

-- ----------------------------------------
-- Task 2～4: データ投入（改善版：MERGE使用）
-- ----------------------------------------
-- 【改善】INSERT → MERGE
-- 
-- MERGE文は、既存データとの照合を行います：
-- 
-- 1. ON句でマッチング条件を指定
--    → customer_id が一致するか確認
-- 
-- 2. WHEN NOT MATCHED の場合
--    → 新規データのみINSERT
-- 
-- 3. 既存データは何もしない
--    → 重複を回避
-- 
-- これにより、何度実行しても同じデータは1回だけ
-- 保存される「べき等性」が実現されます。
-- ----------------------------------------

-- Task 2: 顧客データ投入（MERGE：重複防止）
CREATE OR REPLACE TASK INSERT_INTO_CUSTOMERS
  WAREHOUSE = week81_wh
  AFTER INSERT_INTO_RAW
AS
  MERGE INTO w81_raw_customer t
  USING (
    SELECT parse_json(column1) as data FROM VALUES
    ('{"customer_id": 6, "customer_name": "Frank", "email": "frank@example.com", "created_at": "2024-02-16"}'),
    ('{"customer_id": 7, "customer_name": "Grace", "email": "grace@example.com", "created_at": "2024-02-16"}')
  ) s
  ON t.data:customer_id::NUMBER = s.data:customer_id::NUMBER
  WHEN NOT MATCHED THEN INSERT (data) VALUES (s.data);

-- Task 3: 製品データ投入（MERGE：重複防止）
CREATE OR REPLACE TASK INSERT_INTO_PRODUCT
  WAREHOUSE = week81_wh
  AFTER INSERT_INTO_RAW
AS
  MERGE INTO w81_raw_product t
  USING (
    SELECT parse_json(column1) as data FROM VALUES
    ('{"product_id": 21, "product_name": "Product U", "category": "Electronics", "price": 120.99, "created_at": "2024-02-16"}'),
    ('{"product_id": 22, "product_name": "Product V", "category": "Books", "price": 35.00, "created_at": "2024-02-16"}')
  ) s
  ON t.data:product_id::NUMBER = s.data:product_id::NUMBER
  WHEN NOT MATCHED THEN INSERT (data) VALUES (s.data);

-- Task 4: 売上データ投入（MERGE：重複防止）
CREATE OR REPLACE TASK INSERT_INTO_SALES
  WAREHOUSE = week81_wh
  AFTER INSERT_INTO_RAW
AS
  MERGE INTO w81_raw_sales t
  USING (
    SELECT parse_json(column1) as data FROM VALUES
    ('{"sale_id": 11, "product_id": 21, "customer_id": 6, "quantity": 1, "sale_date": "2024-02-17"}'),
    ('{"sale_id": 12, "product_id": 22, "customer_id": 7, "quantity": 1, "sale_date": "2024-02-17"}'),
    ('{"sale_id": 13, "product_id": 21, "customer_id": 6, "quantity": 2, "sale_date": "2024-02-17"}'),
    ('{"sale_id": 14, "product_id": 22, "customer_id": 7, "quantity": 1, "sale_date": "2024-02-17"}'),
    ('{"sale_id": 15, "product_id": 21, "customer_id": 6, "quantity": 1, "sale_date": "2024-02-17"}')
  ) s
  ON t.data:sale_id::NUMBER = s.data:sale_id::NUMBER
  WHEN NOT MATCHED THEN INSERT (data) VALUES (s.data);

-- Task 5: データ検証（複数親タスク：整合性保証）
CREATE OR REPLACE TASK VALIDATE_DATA
  WAREHOUSE = week81_wh
  AFTER INSERT_INTO_CUSTOMERS, INSERT_INTO_PRODUCT, INSERT_INTO_SALES
AS
  CREATE OR REPLACE TRANSIENT TABLE data_validation_log AS
  SELECT 
    CURRENT_TIMESTAMP() as validation_time,
    (SELECT COUNT(*) FROM w81_raw_customer WHERE data:customer_id::NUMBER IN (6,7)) as new_customers,
    (SELECT COUNT(*) FROM w81_raw_product WHERE data:product_id::NUMBER IN (21,22)) as new_products,
    (SELECT COUNT(*) FROM w81_raw_sales WHERE data:sale_id::NUMBER BETWEEN 11 AND 15) as new_sales,
    CASE 
      WHEN (SELECT COUNT(*) FROM w81_raw_customer WHERE data:customer_id::NUMBER IN (6,7)) = 2
       AND (SELECT COUNT(*) FROM w81_raw_product WHERE data:product_id::NUMBER IN (21,22)) = 2
       AND (SELECT COUNT(*) FROM w81_raw_sales WHERE data:sale_id::NUMBER BETWEEN 11 AND 15) = 5
      THEN 'PASSED'
      ELSE 'FAILED'
    END as validation_status;

-- Task 6: 集約処理（VALIDATE_DATA完了後）
CREATE OR REPLACE TASK AGGREGATE_SALES
  WAREHOUSE = week81_wh
  AFTER VALIDATE_DATA
AS
  CREATE OR REPLACE VIEW aggregated_sales AS
  WITH deduplicated_sales AS (
    SELECT DISTINCT
      data:sale_id::NUMBER as sale_id,
      data:product_id::NUMBER as product_id,
      data:customer_id::NUMBER as customer_id,
      data:quantity::NUMBER as quantity
    FROM w81_raw_sales
  ),
  deduplicated_customers AS (
    SELECT DISTINCT
      data:customer_id::NUMBER as customer_id,
      data:customer_name::VARCHAR as customer_name
    FROM w81_raw_customer
  ),
  deduplicated_products AS (
    SELECT DISTINCT
      data:product_id::NUMBER as product_id,
      data:product_name::VARCHAR as product_name,
      data:price::DECIMAL(10,2) as price
    FROM w81_raw_product
  )  
  SELECT 
    c.customer_name,
    p.product_name,
    SUM(s.quantity) as total_quantity,
    ROUND(SUM(s.quantity * p.price), 2) as total_revenue
  FROM deduplicated_sales s
  INNER JOIN deduplicated_customers c ON s.customer_id = c.customer_id
  INNER JOIN deduplicated_products p ON s.product_id = p.product_id
  GROUP BY c.customer_name, p.product_name;

-- ----------------------------------------
-- タスク有効化と実行
-- ----------------------------------------

ALTER TASK AGGREGATE_SALES RESUME;
ALTER TASK VALIDATE_DATA RESUME;
ALTER TASK INSERT_INTO_SALES RESUME;
ALTER TASK INSERT_INTO_PRODUCT RESUME;
ALTER TASK INSERT_INTO_CUSTOMERS RESUME;

-- 【実行1回目】
EXECUTE TASK INSERT_INTO_RAW;
SELECT SYSTEM$WAIT(5, 'SECONDS');

-- ----------------------------------------
-- 結果確認
-- ----------------------------------------

-- タスクDAG構造確認（複数親タスクを確認）
SELECT 
  t.name,
  COALESCE(ARRAY_SIZE(t.predecessors), 0) as parent_count,
  t.predecessors
FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS(
  task_name => 'INSERT_INTO_RAW', 
  recursive => TRUE
)) t
ORDER BY parent_count, t.name;

-- 期待結果：VALIDATE_DATA の parent_count = 3

-- 検証ログ確認
SELECT * FROM data_validation_log ORDER BY validation_time DESC LIMIT 1;

-- 最終結果
SELECT * FROM aggregated_sales
ORDER BY customer_name, product_name;

-- データ件数確認（1回目）
SELECT 
  'Step 2 - 1回目実行後' as execution,
  (SELECT COUNT(*) FROM w81_raw_customer WHERE data:customer_id::NUMBER IN (6,7)) as new_customers,
  (SELECT COUNT(*) FROM w81_raw_product WHERE data:product_id::NUMBER IN (21,22)) as new_products,
  (SELECT COUNT(*) FROM w81_raw_sales WHERE data:sale_id::NUMBER BETWEEN 11 AND 15) as new_sales,
  (SELECT COUNT(*) FROM aggregated_sales) as aggregated_records;

-- 期待結果：customers=2, products=2, sales=5, aggregated=2

-- ----------------------------------------
-- データの振る舞いを再度確認
-- ----------------------------------------
-- Step 1と同じように、もう一度実行してみます。

EXECUTE TASK INSERT_INTO_RAW;
SELECT SYSTEM$WAIT(5, 'SECONDS');

-- データ件数確認（2回目）
SELECT 
  'Step 2 - 2回目実行後' as execution,
  (SELECT COUNT(*) FROM w81_raw_customer WHERE data:customer_id::NUMBER IN (6,7)) as new_customers,
  (SELECT COUNT(*) FROM w81_raw_product WHERE data:product_id::NUMBER IN (21,22)) as new_products,
  (SELECT COUNT(*) FROM w81_raw_sales WHERE data:sale_id::NUMBER BETWEEN 11 AND 15) as new_sales,
  (SELECT COUNT(*) FROM aggregated_sales) as aggregated_records;

-- ========================================
-- 【結果】データ件数が変わっていません
-- ========================================
-- customers: 2件（変わらず）
-- products: 2件（変わらず）
-- sales: 5件（変わらず）
-- aggregated_records: 2件（変わらず）
-- 
-- 【改善効果】：
-- ✓ MERGEにより、同じデータは追加されない
-- ✓ 何度実行しても結果が同じ（べき等性）
-- ✓ リトライや手動再実行が安全
-- ✓ データ品質の向上
-- 
-- 【実務での意義】：
-- - タスク失敗時のリトライが安全
-- - データの肥大化を防止
-- - クエリパフォーマンスの維持
-- ========================================

-- 検証ログ確認（2回実行されている）
SELECT * FROM data_validation_log ORDER BY validation_time DESC;

-- ========================================
-- Step 2 完了
-- ========================================
-- 【学んだこと】：
-- ✓ MERGEによるべき等性の実現
-- ✓ 複数親タスクによるデータ整合性の保証
-- ✓ より堅牢なタスクDAGの設計
-- 
-- 【次のStep】：
-- Step 3では、この堅牢な実装をそのままに、
-- コストと管理工数を削減する方法を学びます。
-- ========================================

SELECT '★ Step 2完了：MERGEによる重複防止と複数親タスクによる整合性保証を体験' as lesson;


-- ========================================
-- Step 3：コスト最適化（Serverless化）
-- ========================================
-- 【Step 2までの成果】
-- べき等性と整合性を持つ、堅牢なタスクDAGが完成しました。
-- 
-- 【このStepの目的】
-- 機能性はそのままに、コストと管理工数を削減します。
-- 
-- 【改善のポイント】
-- WAREHOUSE = week81_wh
--   ↓
-- USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
-- 
-- この変更により：
-- - ウェアハウス管理が不要
-- - アイドル時間の課金なし
-- - 実行時のみ課金（実行時間1分以内の場合、60～85%削減）
-- 
-- 【今回のケースでの適用条件】
-- ✓ 実行時間：1分以内（短時間）
-- ✓ 即時実行要件：なし（コールドスタート5～30秒許容）
-- ✓ 並列タスク数：少数（大量並列でない）
-- 
-- → Serverlessが最適
-- ========================================

USE ROLE ACCOUNTADMIN;
USE DATABASE FROSTY_FRIDAY;
USE SCHEMA WEEK_081;

-- ----------------------------------------
-- 環境クリア
-- ----------------------------------------

ALTER TASK IF EXISTS INSERT_INTO_RAW SUSPEND;
ALTER TASK IF EXISTS INSERT_INTO_CUSTOMERS SUSPEND;
ALTER TASK IF EXISTS INSERT_INTO_PRODUCT SUSPEND;
ALTER TASK IF EXISTS INSERT_INTO_SALES SUSPEND;
ALTER TASK IF EXISTS VALIDATE_DATA SUSPEND;
ALTER TASK IF EXISTS AGGREGATE_SALES SUSPEND;

DROP TASK IF EXISTS INSERT_INTO_RAW;
DROP TASK IF EXISTS INSERT_INTO_CUSTOMERS;
DROP TASK IF EXISTS INSERT_INTO_PRODUCT;
DROP TASK IF EXISTS INSERT_INTO_SALES;
DROP TASK IF EXISTS VALIDATE_DATA;
DROP TASK IF EXISTS AGGREGATE_SALES;

DROP VIEW IF EXISTS aggregated_sales;
DROP TABLE IF EXISTS data_validation_log;

TRUNCATE TABLE IF EXISTS w81_raw_product;
TRUNCATE TABLE IF EXISTS w81_raw_customer;
TRUNCATE TABLE IF EXISTS w81_raw_sales;

-- ----------------------------------------
-- タスク作成（Serverless版）
-- WAREHOUSE指定なし、USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE使用
-- ----------------------------------------

-- Task 1: ルートタスク（Serverless）
CREATE OR REPLACE TASK INSERT_INTO_RAW
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AS
  SELECT 'Raw data processing initiated' as status;

-- Task 2: 顧客データ投入（Serverless + MERGE）
CREATE OR REPLACE TASK INSERT_INTO_CUSTOMERS
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  AFTER INSERT_INTO_RAW
AS
  MERGE INTO w81_raw_customer t
  USING (
    SELECT parse_json(column1) as data FROM VALUES
    ('{"customer_id": 6, "customer_name": "Frank", "email": "frank@example.com", "created_at": "2024-02-16"}'),
    ('{"customer_id": 7, "customer_name": "Grace", "email": "grace@example.com", "created_at": "2024-02-16"}')
  ) s
  ON t.data:customer_id::NUMBER = s.data:customer_id::NUMBER
  WHEN NOT MATCHED THEN INSERT (data) VALUES (s.data);

-- Task 3: 製品データ投入（Serverless + MERGE）
CREATE OR REPLACE TASK INSERT_INTO_PRODUCT
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  AFTER INSERT_INTO_RAW
AS
  MERGE INTO w81_raw_product t
  USING (
    SELECT parse_json(column1) as data FROM VALUES
    ('{"product_id": 21, "product_name": "Product U", "category": "Electronics", "price": 120.99, "created_at": "2024-02-16"}'),
    ('{"product_id": 22, "product_name": "Product V", "category": "Books", "price": 35.00, "created_at": "2024-02-16"}')
  ) s
  ON t.data:product_id::NUMBER = s.data:product_id::NUMBER
  WHEN NOT MATCHED THEN INSERT (data) VALUES (s.data);

-- Task 4: 売上データ投入（Serverless + MERGE）
CREATE OR REPLACE TASK INSERT_INTO_SALES
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  AFTER INSERT_INTO_RAW
AS
  MERGE INTO w81_raw_sales t
  USING (
    SELECT parse_json(column1) as data FROM VALUES
    ('{"sale_id": 11, "product_id": 21, "customer_id": 6, "quantity": 1, "sale_date": "2024-02-17"}'),
    ('{"sale_id": 12, "product_id": 22, "customer_id": 7, "quantity": 1, "sale_date": "2024-02-17"}'),
    ('{"sale_id": 13, "product_id": 21, "customer_id": 6, "quantity": 2, "sale_date": "2024-02-17"}'),
    ('{"sale_id": 14, "product_id": 22, "customer_id": 7, "quantity": 1, "sale_date": "2024-02-17"}'),
    ('{"sale_id": 15, "product_id": 21, "customer_id": 6, "quantity": 1, "sale_date": "2024-02-17"}')
  ) s
  ON t.data:sale_id::NUMBER = s.data:sale_id::NUMBER
  WHEN NOT MATCHED THEN INSERT (data) VALUES (s.data);

-- Task 5: データ検証（Serverless + 複数親タスク）
CREATE OR REPLACE TASK VALIDATE_DATA
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  AFTER INSERT_INTO_CUSTOMERS, INSERT_INTO_PRODUCT, INSERT_INTO_SALES
AS
  CREATE OR REPLACE TRANSIENT TABLE data_validation_log AS
  SELECT 
    CURRENT_TIMESTAMP() as validation_time,
    (SELECT COUNT(*) FROM w81_raw_customer WHERE data:customer_id::NUMBER IN (6,7)) as new_customers,
    (SELECT COUNT(*) FROM w81_raw_product WHERE data:product_id::NUMBER IN (21,22)) as new_products,
    (SELECT COUNT(*) FROM w81_raw_sales WHERE data:sale_id::NUMBER BETWEEN 11 AND 15) as new_sales,
    CASE 
      WHEN (SELECT COUNT(*) FROM w81_raw_customer WHERE data:customer_id::NUMBER IN (6,7)) = 2
       AND (SELECT COUNT(*) FROM w81_raw_product WHERE data:product_id::NUMBER IN (21,22)) = 2
       AND (SELECT COUNT(*) FROM w81_raw_sales WHERE data:sale_id::NUMBER BETWEEN 11 AND 15) = 5
      THEN 'PASSED'
      ELSE 'FAILED'
    END as validation_status;

-- Task 6: 集約処理（Serverless）
CREATE OR REPLACE TASK AGGREGATE_SALES
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  AFTER VALIDATE_DATA
AS
  CREATE OR REPLACE VIEW aggregated_sales AS
  WITH deduplicated_sales AS (
    SELECT DISTINCT
      data:sale_id::NUMBER as sale_id,
      data:product_id::NUMBER as product_id,
      data:customer_id::NUMBER as customer_id,
      data:quantity::NUMBER as quantity
    FROM w81_raw_sales
  ),
  deduplicated_customers AS (
    SELECT DISTINCT
      data:customer_id::NUMBER as customer_id,
      data:customer_name::VARCHAR as customer_name
    FROM w81_raw_customer
  ),
  deduplicated_products AS (
    SELECT DISTINCT
      data:product_id::NUMBER as product_id,
      data:product_name::VARCHAR as product_name,
      data:price::DECIMAL(10,2) as price
    FROM w81_raw_product
  )  
  SELECT 
    c.customer_name,
    p.product_name,
    SUM(s.quantity) as total_quantity,
    ROUND(SUM(s.quantity * p.price), 2) as total_revenue
  FROM deduplicated_sales s
  INNER JOIN deduplicated_customers c ON s.customer_id = c.customer_id
  INNER JOIN deduplicated_products p ON s.product_id = p.product_id
  GROUP BY c.customer_name, p.product_name;

-- ----------------------------------------
-- タスク有効化と実行
-- ----------------------------------------

ALTER TASK AGGREGATE_SALES RESUME;
ALTER TASK VALIDATE_DATA RESUME;
ALTER TASK INSERT_INTO_SALES RESUME;
ALTER TASK INSERT_INTO_PRODUCT RESUME;
ALTER TASK INSERT_INTO_CUSTOMERS RESUME;

-- タスク状態確認（warehouse列がNULLであることを確認）
SHOW TASKS IN FROSTY_FRIDAY.WEEK_081;

SELECT 
  "name" as task_name,
  "warehouse" as warehouse_setting,
  "state" as current_state
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
ORDER BY "name";

-- 期待結果：warehouse_setting = NULL（Serverlessの証拠）

-- 【実行1回目】
EXECUTE TASK INSERT_INTO_RAW;
SELECT SYSTEM$WAIT(5, 'SECONDS');

-- ----------------------------------------
-- 結果確認
-- ----------------------------------------

-- タスクDAG構造確認
SELECT 
  t.name,
  COALESCE(ARRAY_SIZE(t.predecessors), 0) as parent_count,
  t.predecessors
FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS(
  task_name => 'INSERT_INTO_RAW', 
  recursive => TRUE
)) t
ORDER BY parent_count, t.name;

-- 検証ログ確認
SELECT * FROM data_validation_log ORDER BY validation_time DESC LIMIT 1;

-- 最終結果
SELECT * FROM aggregated_sales
ORDER BY customer_name, product_name;

-- データ件数確認（1回目）
SELECT 
  'Step 3 - 1回目実行後' as execution,
  (SELECT COUNT(*) FROM w81_raw_customer WHERE data:customer_id::NUMBER IN (6,7)) as new_customers,
  (SELECT COUNT(*) FROM w81_raw_product WHERE data:product_id::NUMBER IN (21,22)) as new_products,
  (SELECT COUNT(*) FROM w81_raw_sales WHERE data:sale_id::NUMBER BETWEEN 11 AND 15) as new_sales,
  (SELECT COUNT(*) FROM aggregated_sales) as aggregated_records;

-- ----------------------------------------
-- 【重要】Serverlessでもべき等性確認：2回目実行
-- ----------------------------------------

EXECUTE TASK INSERT_INTO_RAW;
SELECT SYSTEM$WAIT(5, 'SECONDS');

-- データ件数確認（2回目）
SELECT 
  'Step 3 - 2回目実行後' as execution,
  (SELECT COUNT(*) FROM w81_raw_customer WHERE data:customer_id::NUMBER IN (6,7)) as new_customers,
  (SELECT COUNT(*) FROM w81_raw_product WHERE data:product_id::NUMBER IN (21,22)) as new_products,
  (SELECT COUNT(*) FROM w81_raw_sales WHERE data:sale_id::NUMBER BETWEEN 11 AND 15) as new_sales,
  (SELECT COUNT(*) FROM aggregated_sales) as aggregated_records;

-- 期待結果：Step 2と同じく重複なし（customers=2, products=2, sales=5, aggregated=2）

SELECT '★ Step 3完了：Serverlessタスクによるコスト最適化を体験' as lesson;


-- ========================================
-- Step 3 完了：3つのStepの振り返り
-- ========================================

-- ----------------------------------------
-- Step 1: 基本実装
-- ----------------------------------------
-- ✓ タスクDAGの基本を学習
-- ✓ 並列実行の仕組みを理解
-- ✓ データの振る舞いを観察
-- 
-- 気づいた点：
-- - 再実行でデータが増加
-- - 実務での課題を発見

-- ----------------------------------------
-- Step 2: 堅牢化
-- ----------------------------------------
-- ✓ MERGEによるべき等性の実現
-- ✓ 複数親タスクによる整合性保証
-- ✓ 本番環境で安全な実装
-- 
-- 改善効果：
-- - 何度実行しても同じ結果
-- - データ品質の向上

-- ----------------------------------------
-- Step 3: 最適化
-- ----------------------------------------
-- ✓ Serverless化
-- ✓ コスト削減（60～85%）
-- ✓ 管理工数削減
-- 
-- 最終形態：
-- - べき等性 ✓
-- - 整合性 ✓
-- - コスト最適 ✓
-- - 管理容易 ✓

-- ========================================
-- 【実務への適用】
-- ========================================
-- このStep 3の実装が、2024年以降の推奨パターンです。
-- 
-- 適用条件：
-- ✓ 短時間実行（1～5分程度）
-- ✓ 即時実行要件なし
-- ✓ 単発または少数の並列タスク（20個未満）
-- 
-- これらの条件に当てはまる場合、
-- Serverlessタスクで確実にコスト削減効果が得られます。
-- ========================================

SELECT '
3つのStepを通じて、タスクDAGの実装を段階的に改善しました：

Step 1 → 基本を理解
Step 2 → 堅牢性を確保
Step 3 → コストを最適化

この学習パスが、実務でのタスク実装に役立つことを願っています。
' as completion_message;
