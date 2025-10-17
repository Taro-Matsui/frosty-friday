--環境準備
CREATE DATABASE Frosty_Friday
DROP SCHEMA Frosty_Friday.week_073
CREATE SCHEMA Frosty_Friday.week_073

--起動コード
CREATE OR REPLACE table departments (department_name varchar, department_ID int, head_department_ID int);

INSERT INTO departments (department_name, department_ID, head_department_ID) VALUES
    ('Research & Development', 1, NULL),  -- The Research & Development department is the top level.
        ('Product Development', 11, 1),
            ('Software Design', 111, 11),
            ('Product Testing', 112, 11),
        ('Human Resources', 2, 1),
            ('Recruitment', 21, 2),
            ('Employee Relations', 22, 2);


--ANSI標準SQL：CTEsによる実行
WITH RECURSIVE dept_hierarchy AS (
  -- ベースケース: 最上位部門
  SELECT
    department_ID,
    department_name,
    head_department_ID,
    '→ ' || department_name AS full_path,
    1 AS level
  FROM departments
  WHERE head_department_ID IS NULL

  UNION ALL

  -- 再帰ケース: 親部門から子部門を結合
  SELECT
    d.department_ID,
    d.department_name,
    d.head_department_ID,
    dh.full_path || ' → ' || d.department_name AS full_path,
    dh.level + 1 AS level
  FROM departments AS d
  JOIN dept_hierarchy AS dh ON d.head_department_ID = dh.department_ID
)

SELECT
  full_path AS CONNECTION_TREE,
  department_ID AS DEPARTMENT_ID,
  head_department_ID AS HEAD_DEPARTMENT_ID,
  department_name AS DEPARTMENT_NAME
FROM dept_hierarchy
ORDER BY level, department_ID;  -- 階層レベル優先、同階層内はID順



--Oracleローカル関数 CONNECTBYによる回答
SELECT
  SYS_CONNECT_BY_PATH(department_name, ' → ') AS connection_tree,
  department_ID,
  head_department_ID,
  department_name
FROM departments
START WITH head_department_ID IS NULL
CONNECT BY PRIOR department_ID = head_department_ID
ORDER BY department_ID;


