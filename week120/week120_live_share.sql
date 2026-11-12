-- ============================================================================
-- Frosty Friday Week 120  ハンズオン用ワークシート
-- 「エラーは出なかった」合成データ（GENERATE_SYNTHETIC_DATA）は、分析に使えるのか
-- ============================================================================
--
-- 【進め方】
--   ・上から順に、1 文ずつ実行する。SQL の中にカーソルを置いて Ctrl+Enter（Mac は Cmd+Enter）
--   ・「STEP n-m」の番号が付いた文が、実行する単位。番号の順に進めれば、最後まで通る
--   ・1 つの STEP に文が複数あるもの（0-1、0-2、8-1）は、その範囲をまとめて選択してから Ctrl+Enter（選択した範囲が、すべて順に実行される）
--   ・結果が複数の表で返る STEP（SELECT が続くもの）は、画面には最後の結果だけが出る。1 つの STEP には、結果を見る SELECT は 1 つだけにしてある
--   ・実行のたびに、直後の【読み方】と【期待する結果】を見て、画面の数字と見比べる
--   ・数字は実行のたびに少し変わる（合成も LIMIT も毎回同じ結果にはならない）。「期待する結果」は 2026-11 の実測
--
-- 【コメントの見方】
--   【ねらい】   この文で何をするか
--   【待つ間に】 時間がかかる文で、待っている間に話すこと
--   【読み方】   結果の見方。何を測っていて、元とどれだけ違うか
--   【期待する結果】 2026-11 の実測値
--   【言えること】 この結果から言えること
--   【次へ】     次の STEP に進む理由
--
-- 【やり直しのルール】何度でも流し直せる
--   ・表・ビューを作る文は CREATE OR REPLACE（作り直し）。比較表（SCOREBOARD）に足す文（8-12・9-6・10-8・11-12）は、同じ方法の行を置き換える形で、重複しない
--   ・任意の STEP から流し直してよい。ただし「測る」文は、直前の「V_CHECK の向け先」の文（8-9・9-5・10-7）を流した後に流す
--   ・STEP 8-11 は比較表を空にする。流し直すと、9-6・10-8・11-12 の足し直しが必要
--   ・STEP 11-4（LLM の生成）は約 3 分かかり、Cortex のクレジットを使う。結果は毎回変わる（モデルは確率で書くため）。生成の結果を残したいときは、11-4 を流し直さない
--   ・STEP 11-3 の SET LLM はセッション変数。画面を開き直したら、11-3 の SET から流す
--   ・モデルを替えて比べるとき（例：claude-opus-5-5）は、11-3 の SET LLM を替え、11-4 以降を流し直す。比較表の「LLM」行は置き換わるので、残したい方は先に
--       UPDATE FROSTY_DB.W120_UCI.SCOREBOARD SET "方法" = REPLACE("方法", 'LLM ', 'S5.5 ') WHERE "方法" LIKE 'LLM %';
--     のように、行名の先頭を「LLM 」以外に付け替えておく（「LLM 」で始まる行だけが、11-12 で消える。判定表 V_JUDGE は、行名の先頭を見ないので、付け替えた行も判定される）
--
-- 【当日の進め方】登壇（約 15 分）では、実行するのは「お題どおり」の部分だけ。それ以降は、資料で、手順の概要と結果を説明する
--   理由：合成と生成の待ちが長い（GSD の 1 表 約 2〜3 分、3 表 約 3〜4 分、LLM の生成 約 3〜4 分）。すべてを画面で流すと、15 分に収まらない
--   アカウントは 1 つ：GENERATE_SYNTHETIC_DATA は Enterprise 以上で使える。LLM の STEP（STEP 11・14）は AI_COMPLETE が使えるアカウントで流す。
--     検証では、最初 Standard のアカウントで GENERATE_SYNTHETIC_DATA が使えなかった（SCHEMA = DATA_PRIVACY の手続きが見えず、データ保持期間の上限が 1 日だった）。Enterprise に変更して通しで流した。
--   当日にライブで流す文（見出しに「【当日】」が付いている。待ちは、合成の約 40 秒だけ）：0-3（使えるか）→ 1-6（入力の行数）→ 2-1（5 表を合成）→ 2-2（お題の JOIN）→ 3-1（引けた割合）→ 3-2（JOIN の行数）
--   事前に流しておくもの：STEP 1-1〜1-5（入力の 5 表）。STEP 4 以降は、当日は流さない（資料で説明する）。2026-11-11 に、検証に使ったアカウントで STEP 0〜12 を通しで流して確認した
--   STEP 4〜14 は、登壇のあとに、1 文ずつ流して確かめるための手順（持ち帰り用）
--   手順の概要と結果は、資料のページ（資料_縦.html）。
--
-- 【必要なもの】
--   ・Enterprise 以上の Snowflake アカウント（Standard では GENERATE_SYNTHETIC_DATA が見えない）
--   ・SNOWFLAKE_SAMPLE_DATA（TPC-DS）が使えること
--   ・UCI Online Retail II の CSV（STEP 8 で取り込む。CC BY 4.0。入手と CSV 化は STEP 8 に書いた）
--   ・STEP 11（LLM）だけ、AI_COMPLETE が使えるアカウント（トライアルでは使えない。検証に使ったアカウントは、GENERATE_SYNTHETIC_DATA と AI_COMPLETE の両方が使える。使えなければ STEP 11 を飛ばして STEP 12 へ）
--
-- 【全体の流れ】（所要：通しで約 40 分。合成の待ち時間が約 15 分）
--   STEP 0      準備
--   STEP 1〜2   お題を解く（TPC-DS 5 表を合成して JOIN）        → エラーなく通る
--   STEP 3〜5   結果を開く → 入力を見直す → 直す               → 表の繋がりは join_key で直る
--   STEP 6〜7   行の中身を数える → 制約の直し方を比べる         → 式と大小関係は学ばない。直し方で分布が変わる
--   STEP 8〜10  実データ（UCI）で確かめる                       → 分布は残る。対応は崩れる。表を分けると一部は直る
--   STEP 11     LLM に SQL を書かせる                           → 書いた規則は守られ、書かない規則は崩れる。モデルで崩れる場所が変わる
--   STEP 12     比較表と判定表                                  → 結論はここから読む
--   STEP 13     自分のデータで、合成する前に数える 4 つ
-- ============================================================================


-- ============================================================================
-- STEP 0  準備
-- ============================================================================

-- STEP 0-1  ロールとウェアハウス
-- 【ねらい】GENERATE_SYNTHETIC_DATA は Snowpark-optimized の Medium を推奨している。専用のウェアハウスを作る
-- 【待つ間に】ウェアハウスは作るだけで、起動は最初のクエリのとき（約 10 秒）
-- 【注意】FROSTY_DB などを別のロールがすでに持っているアカウントでは、SYSADMIN で作り直せない。そのときは、USE ROLE SYSADMIN の 1 文を流さず、持っているロールで流す（検証に使ったアカウントは ACCOUNTADMIN で流した）
-- 【流し方】3 文をまとめて選択して Ctrl+Enter
USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS FF_W120_WH
  WAREHOUSE_SIZE = 'MEDIUM' WAREHOUSE_TYPE = 'SNOWPARK-OPTIMIZED'
  AUTO_SUSPEND = 60 INITIALLY_SUSPENDED = TRUE;

USE WAREHOUSE FF_W120_WH;

-- STEP 0-2  データベースとスキーマ
-- 【ねらい】この回のオブジェクトは、すべて FROSTY_DB の下に置く（後片付けは、末尾の DROP を流すだけ）
-- 【スキーマの役割】
--   W120_SRC_IND  お題どおりの入力（5 表を別々に LIMIT 5000）   WEEK_120    お題の出力（検証の SQL が、この名前を読む）
--   W120_SRC_FACT 売上起点の入力                                 W120_FACT_JK 売上起点＋join_key の出力
--   W120_N        10 万行にした入力と出力                        W120_STRAT   日付を散らした入力と、制約の直し方
--   W120_UCI      実データ（UCI）と、その合成結果・比較表        W120_LLM     LLM が書いた SQL と、その生成表
-- 【流し方】9 文をまとめて選択して Ctrl+Enter
CREATE DATABASE IF NOT EXISTS FROSTY_DB;

CREATE SCHEMA IF NOT EXISTS FROSTY_DB.W120_SRC_IND;

CREATE SCHEMA IF NOT EXISTS FROSTY_DB.WEEK_120;

CREATE SCHEMA IF NOT EXISTS FROSTY_DB.W120_SRC_FACT;

CREATE SCHEMA IF NOT EXISTS FROSTY_DB.W120_FACT_JK;

CREATE SCHEMA IF NOT EXISTS FROSTY_DB.W120_N;

CREATE SCHEMA IF NOT EXISTS FROSTY_DB.W120_STRAT;

CREATE SCHEMA IF NOT EXISTS FROSTY_DB.W120_UCI;

CREATE SCHEMA IF NOT EXISTS FROSTY_DB.W120_LLM;

-- STEP 0-3 【当日】 GENERATE_SYNTHETIC_DATA が使えるか
-- 【読み方】1 行返れば使える（SCHEMA = DATA_PRIVACY）。0 行なら、このアカウントは Enterprise 未満で、手続き自体が見えない
-- 【期待する結果】1 行。同名で SCHEMA = CORTEX の手続きが見える環境がある。公式の文書に載っていないので使わない
SHOW PROCEDURES LIKE 'GENERATE_SYNTHETIC_DATA' IN ACCOUNT;


-- ============================================================================
-- STEP 1  お題の入力を作る
-- ============================================================================
-- お題の原文は、5 つのビューを作る。
--   CREATE OR REPLACE VIEW WEB_SALES AS (SELECT * FROM SAMPLE_DATA.TPCDS_SF10TCL.WEB_SALES LIMIT 5000);
--   （WEB_RETURNS・WEB_SITE・WEB_PAGE も同じ。WEB_ITEM は TPCDS_SF10TCL.ITEM から）
-- ビューのままだと、LIMIT で取る 5,000 行が実行のたびに変わりうる。結果を比べるため、ここでは「表」に固定する

-- STEP 1-1  売上（5,000 行）
CREATE OR REPLACE TABLE FROSTY_DB.W120_SRC_IND.WEB_SALES AS
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_SALES LIMIT 5000;

-- STEP 1-2  返品（5,000 行）
CREATE OR REPLACE TABLE FROSTY_DB.W120_SRC_IND.WEB_RETURNS AS
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_RETURNS LIMIT 5000;

-- STEP 1-3  サイト
CREATE OR REPLACE TABLE FROSTY_DB.W120_SRC_IND.WEB_SITE AS
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_SITE LIMIT 5000;

-- STEP 1-4  ページ
CREATE OR REPLACE TABLE FROSTY_DB.W120_SRC_IND.WEB_PAGE AS
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_PAGE LIMIT 5000;

-- STEP 1-5  商品（TPC-DS では ITEM という名前）
CREATE OR REPLACE TABLE FROSTY_DB.W120_SRC_IND.WEB_ITEM AS
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.ITEM LIMIT 5000;

-- STEP 1-6 【当日】 入力の行数
-- 【読み方】サイト 78 行、ページ 4,002 行は全件。売上・返品・商品は、元の表の一部を先頭から取っただけ
-- 【期待する結果】売上 5,000 / 返品 5,000 / サイト 78 / ページ 4,002 / 商品 5,000
SELECT 'WEB_SALES' AS "表", COUNT(*) AS "行数" FROM FROSTY_DB.W120_SRC_IND.WEB_SALES
UNION ALL SELECT 'WEB_RETURNS', COUNT(*) FROM FROSTY_DB.W120_SRC_IND.WEB_RETURNS
UNION ALL SELECT 'WEB_SITE', COUNT(*) FROM FROSTY_DB.W120_SRC_IND.WEB_SITE
UNION ALL SELECT 'WEB_PAGE', COUNT(*) FROM FROSTY_DB.W120_SRC_IND.WEB_PAGE
UNION ALL SELECT 'WEB_ITEM', COUNT(*) FROM FROSTY_DB.W120_SRC_IND.WEB_ITEM;


-- ============================================================================
-- STEP 2  お題どおりに合成して、検証の JOIN を流す
-- ============================================================================

-- STEP 2-1 【当日】 5 表を合成する（オプションは何も付けない。お題どおり）
-- 【ねらい】GENERATE_SYNTHETIC_DATA に、入力表と出力表の組を 5 つ渡す
-- 【待つ間に】約 40 秒。仕様を話す：Enterprise 以上、最小 20 行・最大 1,400 万行、1 回に最大 5 表。
--   列ごとの分布と、列どうしの相関を学ぶ方式（コピュラ）。列の指定は join_key・categorical・replace の 3 つ
-- 【読み方】エラーなく終われば、5 つの出力表ができている。戻り値は、元と合成後で列の相関がどれだけ違うかの表
--   （CORRELATION_COEFFICIENT_DIFFERENCE など。ドキュメントは 12 列、2026-11 のトライアルは 11 列だった）
CALL SNOWFLAKE.DATA_PRIVACY.GENERATE_SYNTHETIC_DATA({
  'datasets': [
    {'input_table': 'FROSTY_DB.W120_SRC_IND.WEB_SALES',   'output_table': 'FROSTY_DB.WEEK_120.WEB_SALES'},
    {'input_table': 'FROSTY_DB.W120_SRC_IND.WEB_RETURNS', 'output_table': 'FROSTY_DB.WEEK_120.WEB_RETURNS'},
    {'input_table': 'FROSTY_DB.W120_SRC_IND.WEB_PAGE',    'output_table': 'FROSTY_DB.WEEK_120.WEB_PAGE'},
    {'input_table': 'FROSTY_DB.W120_SRC_IND.WEB_SITE',    'output_table': 'FROSTY_DB.WEEK_120.WEB_SITE'},
    {'input_table': 'FROSTY_DB.W120_SRC_IND.WEB_ITEM',    'output_table': 'FROSTY_DB.WEEK_120.WEB_ITEM'}
  ],
  'replace_output_tables': TRUE
});

-- STEP 2-2 【当日】 お題の検証 SQL（原文のまま）
-- 【読み方】エラーなく結果が返れば、お題としては達成
-- 【期待する結果】結果が返る。ここまでは、誰でも達成できる
-- 【次へ】通った。では、結果の中身は使えるのか。開いて確かめる
SELECT
    ws.*,
    wr.*,
    wp.*,
    wsi.*,
    wi.*
FROM frosty_db.week_120.web_sales ws
LEFT JOIN frosty_db.week_120.web_returns wr
    ON ws.ws_web_page_sk = wr.wr_web_page_sk
    AND ws.ws_item_sk = wr.wr_item_sk
LEFT JOIN frosty_db.week_120.web_page wp
    ON ws.ws_web_page_sk = wp.wp_web_page_sk
LEFT JOIN frosty_db.week_120.web_site wsi
    ON ws.ws_web_site_sk = wsi.web_site_sk
LEFT JOIN frosty_db.week_120.web_item wi
    ON ws.ws_item_sk = wi.i_item_sk;


-- ============================================================================
-- STEP 3  結果を開く：売上から、他の表が引けているか
-- ============================================================================

-- STEP 3-1 【当日】 売上の各行から、商品・ページ・サイトが引ける割合
-- 【測っているもの】売上の各行について、相手の表から行が見つかる割合（1 ＝ 全行で見つかる）
-- 【読み方】合成後の値が、入力と同じなら、表の繋がりは保たれている。下がっていれば、JOIN で相手が消える行が増えている
-- 【期待する結果】
--   入力      ：商品 0.0144 / ページ 1.0 / サイト 1.0
--   合成後    ：商品 0.009 / ページ 0.6 前後 / サイト 0.55〜0.7（実行ごとに変わる）
-- 【言えること】ページ・サイトは 1.0 から 0.6 前後へ落ちた。合成で、売上が持つキーの値と、ページ・サイトの表のキーの値が
--   別々に作り直され、一致しなくなったため。商品は入力の時点で 1.4% しか引けていない（原因は STEP 4）
SELECT '入力（元の5表）' AS "対象",
       ROUND(AVG(IFF(i.k IS NOT NULL, 1, 0)), 4) AS "商品が引けた割合（1が全行）",
       ROUND(AVG(IFF(p.k IS NOT NULL, 1, 0)), 4) AS "ページが引けた割合",
       ROUND(AVG(IFF(s.k IS NOT NULL, 1, 0)), 4) AS "サイトが引けた割合"
FROM FROSTY_DB.W120_SRC_IND.WEB_SALES w
LEFT JOIN (SELECT DISTINCT i_item_sk AS k FROM FROSTY_DB.W120_SRC_IND.WEB_ITEM) i ON w.ws_item_sk = i.k
LEFT JOIN (SELECT DISTINCT wp_web_page_sk AS k FROM FROSTY_DB.W120_SRC_IND.WEB_PAGE) p ON w.ws_web_page_sk = p.k
LEFT JOIN (SELECT DISTINCT web_site_sk AS k FROM FROSTY_DB.W120_SRC_IND.WEB_SITE) s ON w.ws_web_site_sk = s.k
UNION ALL
SELECT '合成後（お題どおり）',
       ROUND(AVG(IFF(i.k IS NOT NULL, 1, 0)), 4),
       ROUND(AVG(IFF(p.k IS NOT NULL, 1, 0)), 4),
       ROUND(AVG(IFF(s.k IS NOT NULL, 1, 0)), 4)
FROM FROSTY_DB.WEEK_120.WEB_SALES w
LEFT JOIN (SELECT DISTINCT i_item_sk AS k FROM FROSTY_DB.WEEK_120.WEB_ITEM) i ON w.ws_item_sk = i.k
LEFT JOIN (SELECT DISTINCT wp_web_page_sk AS k FROM FROSTY_DB.WEEK_120.WEB_PAGE) p ON w.ws_web_page_sk = p.k
LEFT JOIN (SELECT DISTINCT web_site_sk AS k FROM FROSTY_DB.WEEK_120.WEB_SITE) s ON w.ws_web_site_sk = s.k;

-- STEP 3-2 【当日】 JOIN の行数
-- 【測っているもの】お題の検証 JOIN の結果行数。売上 1 行に相手が 1 行ずつ付くなら、行数は売上と同じ 5,000 のまま
-- 【読み方】増えていれば、キーの値が重複して、1 行の売上に複数の相手が付いている（二重計上の原因になる）
-- 【期待する結果】売上 5,000 行に対し、JOIN の結果は 8,957 行（実行ごとに 8,600〜9,000 行前後）
-- 【言えること】エラーなく通っても、JOIN 後の集計（売上合計など）は 1.8 倍に膨らむ。「通った」と「使える」は別
-- 【次へ】合成が悪いのか、入力が悪いのか。入力を開く
SELECT (SELECT COUNT(*) FROM FROSTY_DB.WEEK_120.WEB_SALES) AS "売上の行数（基準）",
       COUNT(*) AS "JOINの行数（売上と同じなら正常）"
FROM FROSTY_DB.WEEK_120.WEB_SALES ws
LEFT JOIN FROSTY_DB.WEEK_120.WEB_RETURNS wr ON ws.ws_web_page_sk = wr.wr_web_page_sk AND ws.ws_item_sk = wr.wr_item_sk
LEFT JOIN FROSTY_DB.WEEK_120.WEB_PAGE wp ON ws.ws_web_page_sk = wp.wp_web_page_sk
LEFT JOIN FROSTY_DB.WEEK_120.WEB_SITE wsi ON ws.ws_web_site_sk = wsi.web_site_sk
LEFT JOIN FROSTY_DB.WEEK_120.WEB_ITEM wi ON ws.ws_item_sk = wi.i_item_sk;


-- ============================================================================
-- STEP 4  入力を開く：商品は 1% しか引けず、返品は 0 件、売上日は 1 日分
-- ============================================================================

-- STEP 4-1  入力の中身
-- 【測っているもの】お題の入力（5 表を別々に LIMIT 5000 で取った表）が、そもそも繋がる形になっているか
-- 【読み方】売上日の種類が 1 なら、日付の分布は学べない。返品が引けた割合が 0 なら、返品表は売上と繋がらない
-- 【期待する結果】売上日の種類 1 / 商品は全 40 万件から 5,000 件を取っただけ / 返品が引けた割合 0.0
-- 【言えること】STEP 3 の「ページ 0.6」は、合成のせいだけではない。入力の 5 表が、互いに重ならない 5,000 行で、
--   商品は 1.4%、返品は 0% しか繋がらない（先頭 5,000 行を取るだけなので、売上は 1 日分）。合成を直す前に、入力を直す必要がある
-- 【次へ】5 表を別々に取るのをやめ、売上を先に取る。繋ぐキーには join_key を付ける
SELECT (SELECT COUNT(DISTINCT ws_sold_date_sk) FROM FROSTY_DB.W120_SRC_IND.WEB_SALES) AS "売上日の種類（1日分）",
       (SELECT COUNT(*) FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.ITEM) AS "商品の全件数（入力は5,000件）",
       ROUND(AVG(IFF(r.k IS NOT NULL, 1, 0)), 4) AS "返品が引けた割合（0＝繋がらない）"
FROM FROSTY_DB.W120_SRC_IND.WEB_SALES w
LEFT JOIN (SELECT DISTINCT wr_web_page_sk AS pk, wr_item_sk AS ik, 1 AS k FROM FROSTY_DB.W120_SRC_IND.WEB_RETURNS) r
  ON w.ws_web_page_sk = r.pk AND w.ws_item_sk = r.ik;


-- ============================================================================
-- STEP 5  直す：売上起点で取り、join_key を指定する
-- ============================================================================

-- STEP 5-1  売上を先に 5,000 行取る
-- 【ねらい】売上を起点にする。キーが空の行は除く（join_key の列に空があると、繋がりが学べない）
CREATE OR REPLACE TABLE FROSTY_DB.W120_SRC_FACT.WEB_SALES AS
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_SALES
WHERE ws_item_sk IS NOT NULL AND ws_web_page_sk IS NOT NULL AND ws_web_site_sk IS NOT NULL
  AND ws_order_number IS NOT NULL AND ws_bill_customer_sk IS NOT NULL AND ws_sold_date_sk IS NOT NULL
LIMIT 5000;

-- STEP 5-2  返品を、売上のキーで絞る
-- 【待つ間に】約 30 秒。返品表は約 7 億行あり、売上の（商品、注文番号）に合う行を探している
CREATE OR REPLACE TABLE FROSTY_DB.W120_SRC_FACT.WEB_RETURNS AS
SELECT r.* FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_RETURNS r
JOIN (SELECT DISTINCT ws_item_sk, ws_order_number FROM FROSTY_DB.W120_SRC_FACT.WEB_SALES) s
  ON r.wr_item_sk = s.ws_item_sk AND r.wr_order_number = s.ws_order_number;

-- STEP 5-3  商品を、売上の商品で絞る
CREATE OR REPLACE TABLE FROSTY_DB.W120_SRC_FACT.WEB_ITEM AS
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.ITEM
WHERE i_item_sk IN (SELECT ws_item_sk FROM FROSTY_DB.W120_SRC_FACT.WEB_SALES);

-- STEP 5-4  ページとサイトは全件
CREATE OR REPLACE TABLE FROSTY_DB.W120_SRC_FACT.WEB_PAGE AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_PAGE;

-- STEP 5-5  サイト
CREATE OR REPLACE TABLE FROSTY_DB.W120_SRC_FACT.WEB_SITE AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_SITE;

-- STEP 5-6  繋ぐ列に join_key を付けて合成する
-- 【ねらい】join_key を付けた列は、表をまたいで「同じ元の値は、同じ合成値」になる。付けない列は、表ごとに別々に作られる
--   （文字列の join_key は UUID で出力される。ここでは数値のキーなので、そのまま数値）
-- 【待つ間に】約 40 秒。STEP 2 との違いは 2 つだけ。入力を「売上起点」にしたことと、繋ぐ列に join_key を付けたこと
CALL SNOWFLAKE.DATA_PRIVACY.GENERATE_SYNTHETIC_DATA({
  'datasets': [
    {'input_table': 'FROSTY_DB.W120_SRC_FACT.WEB_SALES',   'output_table': 'FROSTY_DB.W120_FACT_JK.WEB_SALES',
     'columns': {'WS_ITEM_SK': {'join_key': TRUE}, 'WS_WEB_PAGE_SK': {'join_key': TRUE}, 'WS_WEB_SITE_SK': {'join_key': TRUE}, 'WS_ORDER_NUMBER': {'join_key': TRUE}}},
    {'input_table': 'FROSTY_DB.W120_SRC_FACT.WEB_RETURNS', 'output_table': 'FROSTY_DB.W120_FACT_JK.WEB_RETURNS',
     'columns': {'WR_ITEM_SK': {'join_key': TRUE}, 'WR_WEB_PAGE_SK': {'join_key': TRUE}, 'WR_ORDER_NUMBER': {'join_key': TRUE}}},
    {'input_table': 'FROSTY_DB.W120_SRC_FACT.WEB_PAGE',    'output_table': 'FROSTY_DB.W120_FACT_JK.WEB_PAGE',
     'columns': {'WP_WEB_PAGE_SK': {'join_key': TRUE}}},
    {'input_table': 'FROSTY_DB.W120_SRC_FACT.WEB_SITE',    'output_table': 'FROSTY_DB.W120_FACT_JK.WEB_SITE',
     'columns': {'WEB_SITE_SK': {'join_key': TRUE}}},
    {'input_table': 'FROSTY_DB.W120_SRC_FACT.WEB_ITEM',    'output_table': 'FROSTY_DB.W120_FACT_JK.WEB_ITEM',
     'columns': {'I_ITEM_SK': {'join_key': TRUE}}}
  ],
  'replace_output_tables': TRUE
});

-- STEP 5-7  繋がりが戻ったか
-- 【測っているもの】STEP 3 と同じ「売上から相手が引けた割合」を、売上起点の入力と、join_key ありの合成後で比べる（返品はお題のキー＝ページ＋商品で数える）
-- 【読み方】元と合成後が同じ値なら、繋がりは保たれている。返品が 0.09 なのは、売上の 9% にだけ返品があるため（元の姿）
-- 【期待する結果】
--   元      ：商品 1.0 / ページ 1.0 / サイト 1.0 / 返品 0.09
--   合成後  ：商品 1.0 / ページ 1.0 / サイト 1.0 / 返品 0.09   ← 4 つとも元と一致
-- 【言えること】入力を「売上から先に取り、他表を売上のキーで絞る」形にして、繋ぐ列に join_key を付ければ、繋がりは残る。
--   STEP 3 の落ち込み（ページ 0.6）は、この 2 つの対策で消える
-- 【次へ】繋がった。では、行の中身はどうか
SELECT '元（売上起点の入力）' AS "対象",
       ROUND(AVG(IFF(i.k IS NOT NULL, 1, 0)), 4) AS "商品が引けた割合", ROUND(AVG(IFF(p.k IS NOT NULL, 1, 0)), 4) AS "ページが引けた割合",
       ROUND(AVG(IFF(s.k IS NOT NULL, 1, 0)), 4) AS "サイトが引けた割合", ROUND(AVG(IFF(r.k IS NOT NULL, 1, 0)), 4) AS "返品が引けた割合（元は0.09）"
FROM FROSTY_DB.W120_SRC_FACT.WEB_SALES w
LEFT JOIN (SELECT DISTINCT i_item_sk AS k FROM FROSTY_DB.W120_SRC_FACT.WEB_ITEM) i ON w.ws_item_sk = i.k
LEFT JOIN (SELECT DISTINCT wp_web_page_sk AS k FROM FROSTY_DB.W120_SRC_FACT.WEB_PAGE) p ON w.ws_web_page_sk = p.k
LEFT JOIN (SELECT DISTINCT web_site_sk AS k FROM FROSTY_DB.W120_SRC_FACT.WEB_SITE) s ON w.ws_web_site_sk = s.k
LEFT JOIN (SELECT DISTINCT wr_web_page_sk AS pk, wr_item_sk AS ik, 1 AS k FROM FROSTY_DB.W120_SRC_FACT.WEB_RETURNS) r ON w.ws_web_page_sk = r.pk AND w.ws_item_sk = r.ik
UNION ALL
SELECT '合成後（join_key あり）',
       ROUND(AVG(IFF(i.k IS NOT NULL, 1, 0)), 4), ROUND(AVG(IFF(p.k IS NOT NULL, 1, 0)), 4),
       ROUND(AVG(IFF(s.k IS NOT NULL, 1, 0)), 4), ROUND(AVG(IFF(r.k IS NOT NULL, 1, 0)), 4)
FROM FROSTY_DB.W120_FACT_JK.WEB_SALES w
LEFT JOIN (SELECT DISTINCT i_item_sk AS k FROM FROSTY_DB.W120_FACT_JK.WEB_ITEM) i ON w.ws_item_sk = i.k
LEFT JOIN (SELECT DISTINCT wp_web_page_sk AS k FROM FROSTY_DB.W120_FACT_JK.WEB_PAGE) p ON w.ws_web_page_sk = p.k
LEFT JOIN (SELECT DISTINCT web_site_sk AS k FROM FROSTY_DB.W120_FACT_JK.WEB_SITE) s ON w.ws_web_site_sk = s.k
LEFT JOIN (SELECT DISTINCT wr_web_page_sk AS pk, wr_item_sk AS ik, 1 AS k FROM FROSTY_DB.W120_FACT_JK.WEB_RETURNS) r ON w.ws_web_page_sk = r.pk AND w.ws_item_sk = r.ik;


-- ============================================================================
-- STEP 6  行の中身を数える：式は残るか
-- ============================================================================
-- TPC-DS の売上には、次の式が全行で成り立っている
--   売上額＝売価×数量 ／ 支払額＝売上額−クーポン ／ 純利益＝支払額−原価合計

-- STEP 6-1  式の成立率（5,000 行）
-- 【測っているもの】各行が式を満たしている割合（1＝全行で成り立つ）。許容差は 0.01。あわせて「売価≤定価」と、定価と売価の相関
-- 【読み方】元が 1 なのに合成後が 1 を大きく下回れば、その式は合成後に成り立っていない。
--   相関は、元が 0.7 前後なら、定価が高い商品ほど売価も高い、という関係があった。合成後に 0 へ近づけば、その関係は失われた
-- 【期待する結果】
--   元      ：式 3 本 1.0 / 1.0 / 1.0 ／ 売価≤定価 1.0 ／ 相関 0.702
--   合成後  ：式 3 本 0.0002 / 0.0000 / 0.0000 ／ 売価≤定価 0.74 ／ 相関 0.016（5,000 行中、式が成り立つのは 1〜2 行）
-- 【言えること】1 列ずつの値の分布は学んでも、「売上額は売価×数量」のような列どうしの式は学ばない。
--   売価が定価を超える行が 26% でき、定価と売価の相関は消えた。売上額・支払額・純利益を足し合わせると、合わない金額が出る
SELECT m AS "対象", COUNT(*) AS "行数",
       ROUND(AVG(IFF(ABS(ws_ext_sales_price - ws_sales_price * ws_quantity) < 0.01, 1, 0)), 4) AS "売上額＝売価×数量（元は1）",
       ROUND(AVG(IFF(ABS(ws_net_paid - (ws_ext_sales_price - ws_coupon_amt)) < 0.01, 1, 0)), 4) AS "支払額＝売上額−クーポン（元は1）",
       ROUND(AVG(IFF(ABS(ws_net_profit - (ws_net_paid - ws_ext_wholesale_cost)) < 0.01, 1, 0)), 4) AS "純利益＝支払額−原価合計（元は1）",
       ROUND(AVG(IFF(ws_sales_price <= ws_list_price, 1, 0)), 4) AS "売価≤定価（元は1）",
       ROUND(CORR(ws_list_price, ws_sales_price), 3) AS "定価と売価の相関（元は0.7）"
FROM (SELECT '元' AS m, * FROM FROSTY_DB.W120_SRC_FACT.WEB_SALES
      UNION ALL SELECT '合成後', * FROM FROSTY_DB.W120_FACT_JK.WEB_SALES)
GROUP BY m ORDER BY m DESC;

-- STEP 6-2  5,000 行が少ないせいか？ 10 万行の入力を作る
-- 【ねらい】「件数が少ないから学べないのでは」という疑問に答える。件数を 20 倍にして、同じことをやる
CREATE OR REPLACE TABLE FROSTY_DB.W120_N.SRC_100K AS
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_SALES
WHERE ws_item_sk IS NOT NULL AND ws_bill_customer_sk IS NOT NULL AND ws_sold_date_sk IS NOT NULL
  AND ws_quantity IS NOT NULL AND ws_sales_price IS NOT NULL AND ws_list_price IS NOT NULL AND ws_coupon_amt IS NOT NULL
  AND ws_wholesale_cost IS NOT NULL AND ws_ext_sales_price IS NOT NULL AND ws_net_paid IS NOT NULL
  AND ws_ext_wholesale_cost IS NOT NULL AND ws_net_profit IS NOT NULL
LIMIT 100000;

-- STEP 6-3  10 万行を合成する
-- 【待つ間に】約 1 分。ここまでの STEP で、join_key・Snowpark-optimized のウェアハウスを使った理由を話す
CALL SNOWFLAKE.DATA_PRIVACY.GENERATE_SYNTHETIC_DATA({
  'datasets': [{'input_table': 'FROSTY_DB.W120_N.SRC_100K', 'output_table': 'FROSTY_DB.W120_N.SYN_100K'}],
  'replace_output_tables': TRUE
});

-- STEP 6-4  式の成立率（10 万行）
-- 【測っているもの】STEP 6-1 と同じ指標を、20 倍の 10 万行で測る
-- 【読み方】10 万行にして改善すれば、件数不足が原因。変わらなければ、件数を増やしても直らない
-- 【期待する結果】
--   元（10 万行）    ：式 3 本 1.0 / 1.0 / 1.0 ／ 売価≤定価 1.0 ／ 相関 0.684
--   合成後（10 万行）：式 3 本 0.0001 / 0.0001 / 0.0000 ／ 売価≤定価 0.743 ／ 相関 0.006
-- 【言えること】5,000 行の結果とほぼ同じ。件数を増やしても、列どうしの式と相関は学べない（件数が原因ではない）
-- 【次へ】崩れた「売価≤定価」を、直し方 4 通りで比べる（STEP 7）
SELECT m AS "対象", COUNT(*) AS "行数",
       ROUND(AVG(IFF(ABS(ws_ext_sales_price - ws_sales_price * ws_quantity) < 0.01, 1, 0)), 4) AS "売上額＝売価×数量（元は1）",
       ROUND(AVG(IFF(ABS(ws_net_paid - (ws_ext_sales_price - ws_coupon_amt)) < 0.01, 1, 0)), 4) AS "支払額＝売上額−クーポン（元は1）",
       ROUND(AVG(IFF(ABS(ws_net_profit - (ws_net_paid - ws_ext_wholesale_cost)) < 0.01, 1, 0)), 4) AS "純利益＝支払額−原価合計（元は1）",
       ROUND(AVG(IFF(ws_sales_price <= ws_list_price, 1, 0)), 4) AS "売価≤定価（元は1）",
       ROUND(CORR(ws_list_price, ws_sales_price), 3) AS "定価と売価の相関（元は0.7）"
FROM (SELECT '元（10万行）' AS m, * FROM FROSTY_DB.W120_N.SRC_100K
      UNION ALL SELECT '合成後（10万行）', * FROM FROSTY_DB.W120_N.SYN_100K)
GROUP BY m ORDER BY m;


-- ============================================================================
-- STEP 7  「売価≤定価」「出荷日≥販売日」の直し方を比べる
-- ============================================================================

-- STEP 7-1  日付を散らした入力を作る
-- 【ねらい】売上日が 1 日分だと、日付の制約（出荷日≥販売日）が見えない。2001 年を 12 日おきに 30 日取り、1 日あたり最大 167 行にする
CREATE OR REPLACE TABLE FROSTY_DB.W120_STRAT.SRC AS
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.WEB_SALES
WHERE ws_sold_date_sk BETWEEN 2451911 AND 2452259 AND MOD(ws_sold_date_sk - 2451911, 12) = 0
  AND MOD(ws_order_number, 997) = 0
QUALIFY ROW_NUMBER() OVER (PARTITION BY ws_sold_date_sk ORDER BY ws_order_number, ws_item_sk) <= 167;

-- STEP 7-2  (a) そのまま合成する
-- 【待つ間に】約 30 秒。直し方の候補を挙げる：(b) 後から直す (c) 後から落とす (d) 前に列にして渡す
CALL SNOWFLAKE.DATA_PRIVACY.GENERATE_SYNTHETIC_DATA({
  'datasets': [{'input_table': 'FROSTY_DB.W120_STRAT.SRC', 'output_table': 'FROSTY_DB.W120_STRAT.SYN'}],
  'replace_output_tables': TRUE
});

-- STEP 7-3  (b) 後から直す：売価を定価で頭打ちにし、出荷日を販売日以上に寄せる
CREATE OR REPLACE TABLE FROSTY_DB.W120_STRAT.SYN_FIX AS
SELECT * REPLACE (LEAST(ws_sales_price, ws_list_price) AS ws_sales_price,
                  GREATEST(ws_ship_date_sk, ws_sold_date_sk) AS ws_ship_date_sk)
FROM FROSTY_DB.W120_STRAT.SYN;

-- STEP 7-4  (c) 後から落とす：制約を破った行を捨てる
CREATE OR REPLACE TABLE FROSTY_DB.W120_STRAT.SYN_DROP AS
SELECT * FROM FROSTY_DB.W120_STRAT.SYN WHERE ws_sales_price <= ws_list_price AND ws_ship_date_sk >= ws_sold_date_sk;

-- STEP 7-5  (d) 合成の前に、関係を列にして渡す入力を作る
-- 【ねらい】売価を「定価に対する割合」、出荷日を「販売からの日数」に置き換える。この 2 列は、定価以下・販売日以後という制約を、値の範囲として持つ
CREATE OR REPLACE TABLE FROSTY_DB.W120_STRAT.SRC_T AS
SELECT * EXCLUDE (ws_sales_price, ws_ship_date_sk),
       ws_sales_price / NULLIF(ws_list_price, 0) AS price_ratio,
       ws_ship_date_sk - ws_sold_date_sk AS ship_lag
FROM FROSTY_DB.W120_STRAT.SRC;

-- STEP 7-6  (d) 置き換えた表を合成する
-- 【待つ間に】約 30 秒
CALL SNOWFLAKE.DATA_PRIVACY.GENERATE_SYNTHETIC_DATA({
  'datasets': [{'input_table': 'FROSTY_DB.W120_STRAT.SRC_T', 'output_table': 'FROSTY_DB.W120_STRAT.SYN_T'}],
  'replace_output_tables': TRUE
});

-- STEP 7-7  (d) 合成後に、売価と出荷日を組み立て直す
-- 【ねらい】割合と日数から、元の列に戻す。定価×割合、販売日＋日数
CREATE OR REPLACE TABLE FROSTY_DB.W120_STRAT.SYN_REBUILT AS
SELECT * EXCLUDE (price_ratio, ship_lag),
       ROUND(ws_list_price * price_ratio, 2) AS ws_sales_price,
       ROUND(ws_sold_date_sk + ship_lag) AS ws_ship_date_sk
FROM FROSTY_DB.W120_STRAT.SYN_T;

-- STEP 7-8  5 通りを並べる
-- 【測っているもの】制約（売価≤定価、出荷日≥販売日）を守れているかと、同時に、分布（平均売価・定価との相関・行数）が元から歪んでいないか
-- 【読み方】良い方法は、制約の 2 列が 1 で、平均売価・相関・行数が「元」の行に近い。
--   制約だけ 1 でも、平均売価が元から離れていれば、制約を守るために分布を壊している
-- 【期待する結果】
--   方法                行数   売価≤定価  出荷≥販売  平均売価  相関
--   1 元                4,843  1.0（※）   1.0        50.53     0.689
--   2 合成のまま        4,843  0.7456     0.6554     50.36     −0.007  ← 制約が 25〜35% 破れ、相関が消えた
--   3 後から直す        4,843  0.9992     1.0        37.90     0.312   ← 制約は守れるが、平均売価が 50 → 38 に下がる
--   4 後から落とす      2,366  1.0        1.0        35.62     0.336   ← 制約は守れるが、行が半分になり、平均売価も歪む
--   5 前に列にして渡す  4,843  0.9994     1.0        50.19     0.68    ← 制約も、平均売価・相関・行数も元に近い
--   ※ 0.9998〜0.9996：丸めの誤差で 1〜2 行が 1 から外れる。1.0 と読んでよい
-- 【言えること】後から直す・落とすは、制約を守る代わりに分布を壊す。
--   合成の前に、関係を「定価に対する割合」「販売からの日数」という列にして渡すと、制約も分布も残る。
--   これは、人が「売価は定価以下」「出荷は販売以後」という業務ルールを知っていて、列の形に書いたから直った
-- 【次へ】TPC-DS は性能測定用のデータで、季節性も自然な相関も持たない。似せ方は、実データで確かめる
WITH u AS (
  SELECT '1 元' AS m, ws_sales_price, ws_list_price, ws_ship_date_sk, ws_sold_date_sk FROM FROSTY_DB.W120_STRAT.SRC
  UNION ALL SELECT '2 合成のまま', ws_sales_price, ws_list_price, ws_ship_date_sk, ws_sold_date_sk FROM FROSTY_DB.W120_STRAT.SYN
  UNION ALL SELECT '3 後から直す', ws_sales_price, ws_list_price, ws_ship_date_sk, ws_sold_date_sk FROM FROSTY_DB.W120_STRAT.SYN_FIX
  UNION ALL SELECT '4 後から落とす', ws_sales_price, ws_list_price, ws_ship_date_sk, ws_sold_date_sk FROM FROSTY_DB.W120_STRAT.SYN_DROP
  UNION ALL SELECT '5 前に列にして渡す', ws_sales_price, ws_list_price, ws_ship_date_sk, ws_sold_date_sk FROM FROSTY_DB.W120_STRAT.SYN_REBUILT)
SELECT m AS "方法", COUNT(*) AS "行数（元は4,843）",
       ROUND(AVG(IFF(ws_sales_price <= ws_list_price, 1, 0)), 4) AS "売価≤定価（元は1）",
       ROUND(AVG(IFF(ws_ship_date_sk >= ws_sold_date_sk, 1, 0)), 4) AS "出荷日≥販売日（元は1）",
       ROUND(AVG(ws_sales_price), 2) AS "平均売価（元は50.5）",
       ROUND(CORR(ws_list_price, ws_sales_price), 3) AS "定価と売価の相関（元は0.69）"
FROM u GROUP BY m ORDER BY m;


-- ============================================================================
-- STEP 8  実データに替える：UCI Online Retail II
-- ============================================================================
-- 英国のネット通販の実取引。1,067,371 行、2009-12〜2011-12。CC BY 4.0（Daqing Chen、UCI、DOI 10.24432/C5CG6D）
--
-- 【CSV の用意】（ハンズオンの前に、各自のパソコンで済ませておく）
--   1. https://archive.ics.uci.edu/dataset/502/online+retail+ii から zip をダウンロードして展開（online_retail_II.xlsx）
--   2. 同梱の data_uci/uci_to_csv.py で CSV にする（2 つのシートを結合し、8 列にする。openpyxl が必要）
--        py -3 -I uci_to_csv.py online_retail_II.xlsx online_retail_ii.csv
--      できた CSV は 94MB、1,067,372 行（ヘッダー 1 行を含む）

-- STEP 8-1  CSV を置くステージと、読み込みの形式を作る
-- 【ねらい】読み込みの形式に TRIM_SPACE = TRUE（値の前後の空白を除く）を付ける。付けないと、前後に空白がある商品コードが別の値になり、
--   商品コードが 5,305 種類、商品名の揺れが 0.2322 になる。Snowsight の「データの読み込み」と同じ 5,304 種類・0.2289 に揃えるための指定
-- 【流し方】2 文をまとめて選択して Ctrl+Enter
CREATE STAGE IF NOT EXISTS FROSTY_DB.W120_UCI.STG;

CREATE OR REPLACE FILE FORMAT FROSTY_DB.W120_UCI.FF_CSV
  TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' NULL_IF = ('') EMPTY_FIELD_AS_NULL = TRUE TRIM_SPACE = TRUE;

-- STEP 8-2  【手作業】CSV をステージに置く（SQL では流せない）
--   Snowsight の左メニュー「Catalog」→「Database Explorer」→ FROSTY_DB → W120_UCI → Stages → STG
--   → 右上の「+ Files」→ online_retail_ii.csv を選んで「Upload」（94MB。数十秒）
--   置いたら、次の文で、ステージにファイルが見えることを確かめる
-- 【期待する結果】online_retail_ii.csv が 1 行（サイズ 94,248,800 前後）
LIST @FROSTY_DB.W120_UCI.STG;

-- STEP 8-3  取り込み先の表を作る
CREATE OR REPLACE TABLE FROSTY_DB.W120_UCI.RAW (
  INVOICE VARCHAR, STOCKCODE VARCHAR, DESCRIPTION VARCHAR, QUANTITY NUMBER(10,0),
  INVOICEDATE TIMESTAMP_NTZ, PRICE NUMBER(12,2), CUSTOMER_ID NUMBER(10,0), COUNTRY VARCHAR);

-- STEP 8-4  ステージの CSV を表に読み込む
-- 【待つ間に】約 30 秒。この間に、データの説明をする：英国のギフト雑貨の卸が相手の、ネット通販の請求書明細。
--   請求番号が C で始まる行はキャンセル（返品）。顧客 ID が空の行がある（ゲスト購入など）
-- 【読み方】rows_loaded が 1,067,371 なら、全行入っている。errors_seen が 0 なら、型の変換でエラーは出ていない
COPY INTO FROSTY_DB.W120_UCI.RAW
FROM @FROSTY_DB.W120_UCI.STG
FILE_FORMAT = (FORMAT_NAME = 'FROSTY_DB.W120_UCI.FF_CSV')
ON_ERROR = 'ABORT_STATEMENT';

-- STEP 8-5 元データの大きさと範囲
-- 【測っているもの】元データの件数・期間・欠損。これが「正解」の姿で、以降の合成データは、ここと比べる
-- 【期待する結果】1,067,371 行 / 2009-12-01 07:45 〜 2011-12-09 12:50 / キャンセル行 19,494 / 顧客ID欠損 243,007 /
--   請求書 53,628 / 商品コード 5,304 / 顧客 5,942
SELECT COUNT(*) AS "行数", MIN(INVOICEDATE) AS "最初", MAX(INVOICEDATE) AS "最後",
       COUNT_IF(INVOICE LIKE 'C%') AS "キャンセル行", COUNT_IF(CUSTOMER_ID IS NULL) AS "顧客ID欠損",
       COUNT(DISTINCT INVOICE) AS "請求書", COUNT(DISTINCT STOCKCODE) AS "商品コード", COUNT(DISTINCT CUSTOMER_ID) AS "顧客"
FROM FROSTY_DB.W120_UCI.RAW;

-- STEP 8-6 実データの癖：「C で始まる ⇔ 数量がマイナス」
-- 【測っているもの】「請求番号が C で始まる（キャンセル）⇔ 数量がマイナス」という業務ルールが、元データで何行守られているか
-- 【読み方】全行が規則どおりなら 100%。例外が少しあれば、実データの癖（入力ミス・運用のゆらぎ）
-- 【期待する結果】規則どおり 1,063,913 行（99.68%）／ 例外 3,458 行。完全には揃っていない。
--   合成データでも、これと同じ 99.7% が目標になる
SELECT COUNT_IF((INVOICE LIKE 'C%') = (QUANTITY < 0)) AS "規則どおりの行",
       COUNT_IF((INVOICE LIKE 'C%') <> (QUANTITY < 0)) AS "例外の行"
FROM FROSTY_DB.W120_UCI.RAW;

-- STEP 8-7  数行見る
-- 【読み方】同じ請求番号の行は、日時・顧客・国が同じ。同じ商品コードは、同じ商品名。この「行どうしの対応」が、合成後に残るかを、これから見る
SELECT * FROM FROSTY_DB.W120_UCI.RAW ORDER BY INVOICEDATE, INVOICE LIMIT 8;

-- STEP 8-8  元データの表を作る
-- 【ねらい】合計金額の列が無いので、数量×単価の列（AMOUNT）を足した表を「元データ」とする。合成の入力も、これを使う
CREATE OR REPLACE TABLE FROSTY_DB.W120_UCI.SRC AS
SELECT *, ROUND(QUANTITY * PRICE, 2) AS AMOUNT FROM FROSTY_DB.W120_UCI.RAW;

-- ----------------------------------------------------------------------------
-- 比べる測定SQLを用意する
--   V_CHECK  : 今、測りたい表を指すビュー。方法ごとに、向け先を替える
--   V_METRICS: V_CHECK を測る測定SQL（11 項目と行数）。方法ごとに 1 行を、SCOREBOARD（比較表）に足していく
--
-- 11 項目と行数が測っているもの（「元」の値が、合成データの目標）
--   項目                          測っていること                                 元の値    外れると何が困るか
--   金額＝数量×単価               列どうしの計算式が成り立つ行の割合              1.0       売上の合計が合わなくなる
--   日時が複数ある請求書          同じ請求番号で日時が複数ある請求書の割合        0.0015    1 つの注文が、複数の時刻に分かれて見える
--   商品名が複数ある商品コード    同じ商品コードに複数の商品名が付く割合          0.229     商品別の集計で、同じ商品が別物になる
--   国が複数ある顧客              同じ顧客が複数の国にまたがる割合                0.0022    顧客別・国別の分析が成り立たない
--   キャンセル行の割合            請求番号が C の行の割合                         0.0183    返品率が、実際より高く（低く）見える
--   C⇔マイナス数量                C なら数量がマイナス、の規則の成立率            0.997     キャンセルの数量が正になり、売上が過大になる
--   月別の件数の形（元との相関）  月ごとの件数の山谷が、元と同じ形か（1＝同じ）     1.0       季節性の分析ができなくなる
--   営業時間外の割合              7 時前・21 時以降の件数の割合                   0.00004   営業時間の分析が成り立たない（元は 42 行だけ）
--   土曜の割合                    土曜日の件数の割合（元は土曜に営業しない）      0.0004    曜日別の分析が成り立たない
--   請求書あたりの行数            1 請求書あたりの平均行数                        19.9      バスケット分析（併売）ができなくなる
--   商品コードの種類              商品コードの異なり数                            5,304     商品の幅が変わる
--   行数                          全体の行数                                      1,067,371 規模の違い
-- ----------------------------------------------------------------------------

-- STEP 8-9  測る表の向け先（まず元データ）
CREATE OR REPLACE VIEW FROSTY_DB.W120_UCI.V_CHECK AS SELECT * FROM FROSTY_DB.W120_UCI.SRC;

-- STEP 8-10  測定SQL（11 項目と行数）
-- 【ねらい】どの方法の表でも、同じ定義で測る。V_CHECK の向け先を替えて、何度でも流せる
CREATE OR REPLACE VIEW FROSTY_DB.W120_UCI.V_METRICS AS
WITH t AS (
  SELECT INVOICE::VARCHAR AS INVOICE, STOCKCODE::VARCHAR AS STOCKCODE, DESCRIPTION::VARCHAR AS DESCRIPTION,
         QUANTITY::NUMBER(18,2) AS QUANTITY, INVOICEDATE::TIMESTAMP_NTZ AS INVOICEDATE, PRICE::NUMBER(18,4) AS PRICE,
         CUSTOMER_ID::NUMBER AS CUSTOMER_ID, COUNTRY::VARCHAR AS COUNTRY, AMOUNT::NUMBER(18,4) AS AMOUNT
  FROM FROSTY_DB.W120_UCI.V_CHECK)
SELECT
  (SELECT COUNT(*) FROM t) AS "行数",
  (SELECT ROUND(AVG(IFF(ABS(AMOUNT - QUANTITY * PRICE) < 0.015, 1, 0)), 4) FROM t) AS "金額＝数量×単価",
  (SELECT ROUND(AVG(IFF(c > 1, 1, 0)), 4) FROM (SELECT COUNT(DISTINCT INVOICEDATE) AS c FROM t GROUP BY INVOICE)) AS "日時が複数ある請求書",
  (SELECT ROUND(AVG(IFF(c > 1, 1, 0)), 4) FROM (SELECT COUNT(DISTINCT DESCRIPTION) AS c FROM t GROUP BY STOCKCODE)) AS "商品名が複数ある商品コード",
  (SELECT ROUND(AVG(IFF(c > 1, 1, 0)), 4) FROM (SELECT COUNT(DISTINCT COUNTRY) AS c FROM t WHERE CUSTOMER_ID IS NOT NULL GROUP BY CUSTOMER_ID)) AS "国が複数ある顧客",
  (SELECT ROUND(AVG(IFF(INVOICE LIKE 'C%', 1, 0)), 4) FROM t) AS "キャンセル行の割合",
  (SELECT ROUND(AVG(IFF((INVOICE LIKE 'C%') = (QUANTITY < 0), 1, 0)), 4) FROM t) AS "C⇔マイナス数量",
  (SELECT ROUND(CORR(g.n, s.n), 3)
     FROM (SELECT TO_CHAR(INVOICEDATE, 'YYYY-MM') AS ym, COUNT(*) AS n FROM t GROUP BY 1) g
     JOIN (SELECT TO_CHAR(INVOICEDATE, 'YYYY-MM') AS ym, COUNT(*) AS n FROM FROSTY_DB.W120_UCI.SRC GROUP BY 1) s ON g.ym = s.ym) AS "月別の件数の形（元との相関）",
  (SELECT ROUND(AVG(IFF(HOUR(INVOICEDATE) < 7 OR HOUR(INVOICEDATE) > 20, 1, 0)), 4) FROM t) AS "営業時間外の割合",
  (SELECT ROUND(AVG(IFF(DAYOFWEEK(INVOICEDATE) = 6, 1, 0)), 4) FROM t) AS "土曜の割合",
  (SELECT ROUND(COUNT(*) / COUNT(DISTINCT INVOICE), 1) FROM t) AS "請求書あたりの行数",
  (SELECT COUNT(DISTINCT STOCKCODE) FROM t) AS "商品コードの種類";

-- STEP 8-11  比較表の入れ物（空）
CREATE OR REPLACE TABLE FROSTY_DB.W120_UCI.SCOREBOARD AS
SELECT 'x'::VARCHAR AS "方法", * FROM FROSTY_DB.W120_UCI.V_METRICS WHERE 1 = 0;

-- STEP 8-12  元データを測って、比較表の 1 行目にする
-- 【ねらい】測定SQLの確認を兼ねる。元データを測った値が、STEP 8-5・8-6 で見た値と合っているか
-- 【期待する結果】行数 1,067,371 / 金額 1 / 日時が複数ある請求書 0.0015 / 商品名が複数ある商品コード 0.2289 / 国が複数ある顧客 0.0022 /
--   キャンセル行 0.0183 / C⇔マイナス 0.9968 / 月別の形 1 / 営業時間外 0.0000（42 行） / 土曜 0.0004 / 請求書あたり 19.9 / 商品コード 5,304
-- 【やり直し】何度流しても 1 行になる（同じ方法の行を消してから足す形）。V_CHECK の向け先は STEP 8-9 のまま（元データ）であること
CREATE OR REPLACE TABLE FROSTY_DB.W120_UCI.SCOREBOARD AS
SELECT * FROM FROSTY_DB.W120_UCI.SCOREBOARD WHERE "方法" <> '元データ'
UNION ALL SELECT '元データ', * FROM FROSTY_DB.W120_UCI.V_METRICS;

-- STEP 8-13  比較表を見る（今は 1 行）
SELECT * FROM FROSTY_DB.W120_UCI.SCOREBOARD;


-- ============================================================================
-- STEP 9  そのまま合成する：1 表・107 万行
-- ============================================================================

-- STEP 9-1  合成する（オプションは何も付けない。1 つの表に、9 列＝ AMOUNT を含む）
-- 【待つ間に】約 2 分。ここで、STEP 6〜7 の結果から予想を言う：
--   「1 列ずつの分布は残るはず。列どうしの対応は、また崩れるはず」。この予想を、次の 4 つのクエリで確かめる
CALL SNOWFLAKE.DATA_PRIVACY.GENERATE_SYNTHETIC_DATA({
  'datasets': [{'input_table': 'FROSTY_DB.W120_UCI.SRC', 'output_table': 'FROSTY_DB.W120_UCI.SYN'}],
  'replace_output_tables': TRUE
});

-- STEP 9-2 月ごとの件数
-- 【測っているもの】月ごとの件数。元の季節性（11 月の山、年末の落ち込み）が、合成で残っているか
-- 【読み方】「差の割合」が 0 に近いほど、元の形が残っている。±10% 以内なら、形は同じと見てよい
-- 【期待する結果】25 か月とも元と ±13% 以内（元との相関 0.99）
-- 【言えること】GSD は、1 列ずつの分布（月ごとの山谷）を自動で学ぶ。ここまでは、合成データが分析に使える
SELECT TO_CHAR(INVOICEDATE, 'YYYY-MM') AS "月",
       COUNT_IF(m = '元') AS "元", COUNT_IF(m = '合成') AS "合成",
       ROUND((COUNT_IF(m = '合成') - COUNT_IF(m = '元')) / COUNT_IF(m = '元') * 100, 1) AS "差の割合（％）"
FROM (SELECT '元' AS m, INVOICEDATE FROM FROSTY_DB.W120_UCI.SRC UNION ALL SELECT '合成', INVOICEDATE FROM FROSTY_DB.W120_UCI.SYN)
GROUP BY 1 ORDER BY 1;

-- STEP 9-3  時間帯別の件数
-- 【測っているもの】時間帯別の件数。月別のように、日時の中身も残るか
-- 【読み方】元は 9〜17 時が中心で、夜間はほぼゼロ。合成でも「元」と同じ列がゼロに近ければ、営業時間の形が残っている
-- 【期待する結果】合成は夜間も毎時 2.8 万行前後で埋まる（昼のピークは残る）
-- 【言えること】月の山は残っても、時刻は元の「営業しない時間」を学べず、一様に埋まる。
--   時刻で分析するなら、この合成は元と違う（元の行にない時間帯のデータができる）
SELECT HOUR(INVOICEDATE) AS "時",
       COUNT_IF(m = '元') AS "元", COUNT_IF(m = '合成') AS "合成"
FROM (SELECT '元' AS m, INVOICEDATE FROM FROSTY_DB.W120_UCI.SRC UNION ALL SELECT '合成', INVOICEDATE FROM FROSTY_DB.W120_UCI.SYN)
GROUP BY 1 ORDER BY 1;

-- STEP 9-4 曜日別の件数
-- 【読み方】番号 0 が日曜、6 が土曜。元は土曜にほぼ営業しない（402 行）
-- 【期待する結果】合成は土曜が 5 万行台。元は 402 行
SELECT DAYNAME(INVOICEDATE) AS "曜日", DAYOFWEEK(INVOICEDATE) AS "番号",
       COUNT_IF(m = '元') AS "元", COUNT_IF(m = '合成') AS "合成"
FROM (SELECT '元' AS m, INVOICEDATE FROM FROSTY_DB.W120_UCI.SRC UNION ALL SELECT '合成', INVOICEDATE FROM FROSTY_DB.W120_UCI.SYN)
GROUP BY 1, 2 ORDER BY 2;

-- STEP 9-5 測定SQLの向け先を、合成した表に替える
CREATE OR REPLACE VIEW FROSTY_DB.W120_UCI.V_CHECK AS SELECT * FROM FROSTY_DB.W120_UCI.SYN;

-- STEP 9-6 11 項目と行数を測って、比較表に「GSD 1表のまま」の行を足す
-- 【やり直し】何度流しても 1 行になる。STEP 9-5 で V_CHECK を SYN に向けた後に流すこと
CREATE OR REPLACE TABLE FROSTY_DB.W120_UCI.SCOREBOARD AS
SELECT * FROM FROSTY_DB.W120_UCI.SCOREBOARD WHERE "方法" <> 'GSD 1表のまま'
UNION ALL SELECT 'GSD 1表のまま', * FROM FROSTY_DB.W120_UCI.V_METRICS;

-- STEP 9-7  比較表を見る（元データと 1 表のままの 2 行）
-- 【読み方】元データの行と同じ値なら、その項目は元に近い。大きく違えば、その項目は元と違う
-- 【期待する結果】金額 0.0063（元 1.0）/ 日時が複数ある請求書 0.95（元 0.0015）/ 商品名が複数ある商品コード 0.98（元 0.23）/
--   国が複数ある顧客 0.99（元 0.0022）/ キャンセル行 0.07（元 0.018）/ 月別の形 0.99（元 1.0）/ 営業時間外 0.27（元 0.00004）
-- 【言えること】1 表のまま流すと、行の中の対応（同じ請求書は同じ日時、同じ商品コードは同じ商品名、同じ顧客は同じ国）が、
--   ほとんど元と違う（95〜99% の請求書・商品・顧客で違う）。月別の形だけが元に近い
-- 【次へ】お題で効いた join_key を、ここでも使う。表を分ける
SELECT * FROM FROSTY_DB.W120_UCI.SCOREBOARD;


-- ============================================================================
-- STEP 10  3 表に分けて合成する
-- ============================================================================
-- 請求書（1 請求書 1 行）・明細（1 行 1 品目）・商品（1 商品コード 1 行）に分け、請求番号と商品コードを join_key にする。
--   ・キャンセルかどうかは請求書に持たせる（IS_CANCEL）。数量は絶対値で持ち、符号は組み立てで決める
--   ・文字列の join_key は UUID で出力される。元の請求番号の形（6 桁・先頭 C）は残らないので、組み立てで付け直す

-- STEP 10-1  請求書の表（53,628 行）
CREATE OR REPLACE TABLE FROSTY_DB.W120_UCI.T_INV AS
SELECT INVOICE, MIN(INVOICEDATE) AS INVOICEDATE, ANY_VALUE(CUSTOMER_ID) AS CUSTOMER_ID, ANY_VALUE(COUNTRY) AS COUNTRY,
       MAX(IFF(INVOICE LIKE 'C%', 1, 0)) AS IS_CANCEL
FROM FROSTY_DB.W120_UCI.SRC GROUP BY INVOICE;

-- STEP 10-2  明細の表（1,067,371 行）
CREATE OR REPLACE TABLE FROSTY_DB.W120_UCI.T_LINE AS
SELECT INVOICE, STOCKCODE, ABS(QUANTITY) AS QTY_ABS, PRICE FROM FROSTY_DB.W120_UCI.SRC;

-- STEP 10-3  商品の表（5,304 行）。商品名は、商品コードごとに最頻値を 1 つ持つ
CREATE OR REPLACE TABLE FROSTY_DB.W120_UCI.T_PRODUCT AS
SELECT STOCKCODE, MODE(DESCRIPTION) AS DESCRIPTION FROM FROSTY_DB.W120_UCI.SRC GROUP BY STOCKCODE;

-- STEP 10-4  3 表をまとめて合成する
-- 【待つ間に】約 3 分。STEP 5 の join_key の話を思い出す：「請求番号」と「商品コード」が、3 表を繋ぐキー
CALL SNOWFLAKE.DATA_PRIVACY.GENERATE_SYNTHETIC_DATA({'datasets': [
  {'input_table': 'FROSTY_DB.W120_UCI.T_INV',     'output_table': 'FROSTY_DB.W120_UCI.S_INV',
   'columns': {'INVOICE': {'join_key': TRUE}}},
  {'input_table': 'FROSTY_DB.W120_UCI.T_LINE',    'output_table': 'FROSTY_DB.W120_UCI.S_LINE',
   'columns': {'INVOICE': {'join_key': TRUE}, 'STOCKCODE': {'join_key': TRUE}}},
  {'input_table': 'FROSTY_DB.W120_UCI.T_PRODUCT', 'output_table': 'FROSTY_DB.W120_UCI.S_PRODUCT',
   'columns': {'STOCKCODE': {'join_key': TRUE}, 'DESCRIPTION': {'categorical': TRUE}}}
], 'replace_output_tables': TRUE});

-- STEP 10-5  合成した 3 表を JOIN して、元と同じ形の 1 表に組み立てる
-- 【ねらい】請求書の日時・顧客・国は請求書の表から、商品名は商品の表から取る。キャンセルは請求番号の先頭 C と数量の符号で戻す
CREATE OR REPLACE TABLE FROSTY_DB.W120_UCI.S_FLAT AS
SELECT IFF(i.IS_CANCEL = 1, 'C', '') || (DENSE_RANK() OVER (ORDER BY i.INVOICE) + 489000) AS INVOICE,
       l.STOCKCODE AS STOCKCODE,
       p.DESCRIPTION AS DESCRIPTION,
       IFF(i.IS_CANCEL = 1, -1, 1) * l.QTY_ABS AS QUANTITY,
       i.INVOICEDATE AS INVOICEDATE,
       l.PRICE AS PRICE,
       i.CUSTOMER_ID AS CUSTOMER_ID,
       i.COUNTRY AS COUNTRY,
       ROUND(IFF(i.IS_CANCEL = 1, -1, 1) * l.QTY_ABS * l.PRICE, 2) AS AMOUNT
FROM FROSTY_DB.W120_UCI.S_LINE l
JOIN FROSTY_DB.W120_UCI.S_INV i ON l.INVOICE = i.INVOICE
LEFT JOIN FROSTY_DB.W120_UCI.S_PRODUCT p ON l.STOCKCODE = p.STOCKCODE;

-- STEP 10-6  JOIN で欠けていないか
-- 【測っているもの】3 表を JOIN して 1 表に戻したとき、行や商品名が欠けていないか
-- 【読み方】組み立て後の行数が明細と同じなら、JOIN で行は欠けていない。商品名が空の行は、元の商品表で商品名が空のコード（355 個）が、そのまま残ったもの
-- 【期待する結果】明細 1,067,371 行 / 請求書 53,628 / 商品 5,304 / 組み立て後 1,067,371 行 / 商品名が空の行 70,093（元データでは 4,382 行）
-- 【言えること】行は欠けていない。ただし、商品名が空のコードが明細に出る行数は、元の 0.4% から 6.6% に増えた。
--   合成後の明細では、商品コードが元ほど偏って選ばれず、商品名が空のコードにも行が割り当てられるため（原因は未検証）
SELECT (SELECT COUNT(*) FROM FROSTY_DB.W120_UCI.S_INV) AS "請求書",
       (SELECT COUNT(*) FROM FROSTY_DB.W120_UCI.S_LINE) AS "明細",
       (SELECT COUNT(*) FROM FROSTY_DB.W120_UCI.S_PRODUCT) AS "商品",
       (SELECT COUNT(*) FROM FROSTY_DB.W120_UCI.S_FLAT) AS "組み立て後の行数",
       (SELECT COUNT_IF(DESCRIPTION IS NULL) FROM FROSTY_DB.W120_UCI.S_FLAT) AS "商品名が空の行";

-- STEP 10-7 測定SQLの向け先を、組み立てた表に替える
CREATE OR REPLACE VIEW FROSTY_DB.W120_UCI.V_CHECK AS SELECT * FROM FROSTY_DB.W120_UCI.S_FLAT;

-- STEP 10-8 11 項目と行数を測って、比較表に「GSD 3表に分割」の行を足す
-- 【やり直し】何度流しても 1 行になる。STEP 10-7 で V_CHECK を S_FLAT に向けた後に流すこと
CREATE OR REPLACE TABLE FROSTY_DB.W120_UCI.SCOREBOARD AS
SELECT * FROM FROSTY_DB.W120_UCI.SCOREBOARD WHERE "方法" <> 'GSD 3表に分割'
UNION ALL SELECT 'GSD 3表に分割', * FROM FROSTY_DB.W120_UCI.V_METRICS;

-- STEP 10-9  比較表を見る（元データ・1 表のまま・3 表に分割の 3 行）
-- 【読み方】「1 表のまま」の行と見比べる。良くなった項目は、表の分け方が効いている。悪くなった項目は、分けたことの副作用
-- 【期待する結果】
--   直ったもの（構成で決まる）: 金額 1 / 日時が複数ある請求書 0（元 0.0015）/ C⇔マイナス 1（元 0.997）
--   元と違うが整ったもの    : 商品名が複数ある商品コード 0（元 0.23。1 コード 1 名にしたので、元の揺れは再現しない）
--   新しく崩れたもの        : キャンセル行 0.15（元 0.018）/ 国が複数ある顧客 0.40〜0.41（元 0.002）
--   直らないもの            : 営業時間外 0.27 / 土曜 0.05
--   下がったもの            : 月別の件数の形 0.84〜0.87（1 表のままでは 0.99。請求書の日時を 1 表で学ぶより、請求書単位で学ぶほうが、月の山が崩れる）
-- 【言えること】直ったのは、GSD が学んだからではなく、請求書・商品を 1 行ずつの表に分けて、組み立て方で決めたから。
--   一方、分けたことで、請求書にキャンセルの割合が引き継がれず（0.018 → 0.15）、顧客と国の対応も崩れた
-- 【次へ】記事⑤で期待した LLM は、これらを直せるか
SELECT * FROM FROSTY_DB.W120_UCI.SCOREBOARD;


-- ============================================================================
-- STEP 11  LLM に、データを作る SQL を書かせる  ※ AI_COMPLETE が使えるアカウントだけ
-- ============================================================================
-- 元の行は LLM に渡さない。渡すのは「統計」だけ（JSON 約 4KB）。LLM が書いた SQL を実行して、元と同じ 1,067,371 行を作る
--   A：統計だけを渡す
--   B：統計に、業務ルールの文章を足して渡す
-- 各 3 回。モデルは claude-sonnet-5-5、temperature 0.7。LLM が書いた SQL は、検査を通ったものだけ実行する
-- （検査：決めた表を作る 1 文で始まる／セミコロンを含まない／DROP・DELETE・UPDATE などの語を含まない。GET( のように、関数として呼ぶ形は対象外）
-- ※ 処理リージョンは CORTEX_ENABLED_CROSS_REGION で決まる。渡すのは統計だけで、元の行は送らない
-- ※ 使えるアカウントでなければ、STEP 11 は全部飛ばして STEP 12 へ進む（比較表は 3 行のまま）

-- STEP 11-0  AI_COMPLETE が使えるか
-- 【読み方】文章が返れば使える。「not available」「Unknown function」などのエラーなら、このアカウントでは使えない（STEP 11 を飛ばす）
SELECT AI_COMPLETE('claude-sonnet-5-5', 'OK とだけ答えてください') AS "応答";

-- STEP 11-1  LLM に渡す統計を作る
-- 【ねらい】元の行は渡さず、集計だけを JSON 1 つにまとめる。月・曜日・時間帯の割合、国の上位、数量・単価の分位点、
--   請求書あたりの行数、キャンセルの割合など
CREATE OR REPLACE TABLE FROSTY_DB.W120_LLM.PROFILE AS
WITH t AS (SELECT * FROM FROSTY_DB.W120_UCI.RAW), n AS (SELECT COUNT(*) AS c FROM t)
SELECT TO_JSON(OBJECT_CONSTRUCT(
 'rows', (SELECT c FROM n),
 'distinct', (SELECT OBJECT_CONSTRUCT('invoice', COUNT(DISTINCT INVOICE), 'stockcode', COUNT(DISTINCT STOCKCODE), 'customer', COUNT(DISTINCT CUSTOMER_ID), 'country', COUNT(DISTINCT COUNTRY)) FROM t),
 'date_range', (SELECT OBJECT_CONSTRUCT('min', TO_CHAR(MIN(INVOICEDATE)), 'max', TO_CHAR(MAX(INVOICEDATE))) FROM t),
 'month_share', (SELECT OBJECT_AGG(ym, sh) FROM (SELECT TO_CHAR(INVOICEDATE, 'YYYY-MM') AS ym, ROUND(COUNT(*) / (SELECT c FROM n), 4) AS sh FROM t GROUP BY 1)),
 'weekday_share_0_is_sunday', (SELECT OBJECT_AGG(d::STRING, sh) FROM (SELECT DAYOFWEEK(INVOICEDATE) AS d, ROUND(COUNT(*) / (SELECT c FROM n), 4) AS sh FROM t GROUP BY 1)),
 'hour_share', (SELECT OBJECT_AGG(h::STRING, sh) FROM (SELECT HOUR(INVOICEDATE) AS h, ROUND(COUNT(*) / (SELECT c FROM n), 5) AS sh FROM t GROUP BY 1)),
 'country_share_top10', (SELECT OBJECT_AGG(COUNTRY, sh) FROM (SELECT COUNTRY, ROUND(COUNT(*) / (SELECT c FROM n), 4) AS sh FROM t GROUP BY 1 ORDER BY 2 DESC LIMIT 10)),
 'customer_null_share', (SELECT ROUND(AVG(IFF(CUSTOMER_ID IS NULL, 1, 0)), 4) FROM t),
 'customer_id_range', (SELECT OBJECT_CONSTRUCT('min', MIN(CUSTOMER_ID), 'max', MAX(CUSTOMER_ID)) FROM t),
 'quantity', (SELECT OBJECT_CONSTRUCT('negative_share', ROUND(AVG(IFF(QUANTITY < 0, 1, 0)), 4), 'p01', PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY QUANTITY), 'p05', PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY QUANTITY), 'p25', PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY QUANTITY), 'p50', PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY QUANTITY), 'p75', PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY QUANTITY), 'p95', PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY QUANTITY), 'p99', PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY QUANTITY), 'min', MIN(QUANTITY), 'max', MAX(QUANTITY)) FROM t),
 'price', (SELECT OBJECT_CONSTRUCT('le0_share', ROUND(AVG(IFF(PRICE <= 0, 1, 0)), 4), 'p05', PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY PRICE), 'p25', PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY PRICE), 'p50', PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY PRICE), 'p75', PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY PRICE), 'p95', PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY PRICE), 'p99', PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY PRICE), 'max', MAX(PRICE)) FROM t),
 'cancel_invoice_line_share', (SELECT ROUND(AVG(IFF(INVOICE LIKE 'C%', 1, 0)), 4) FROM t),
 'lines_per_invoice', (SELECT OBJECT_CONSTRUCT('avg', ROUND(AVG(c), 1), 'p50', MEDIAN(c), 'p90', PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY c), 'max', MAX(c)) FROM (SELECT COUNT(*) AS c FROM t GROUP BY INVOICE)),
 'stockcode_top20_share', (SELECT OBJECT_AGG(STOCKCODE, sh) FROM (SELECT STOCKCODE, ROUND(COUNT(*) / (SELECT c FROM n), 5) AS sh FROM t GROUP BY 1 ORDER BY 2 DESC LIMIT 20)),
 'description_samples', (SELECT ARRAY_AGG(DESCRIPTION) FROM (SELECT DESCRIPTION FROM t WHERE DESCRIPTION IS NOT NULL GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 6)),
 'formats', 'INVOICE は6桁の数字で、キャンセルは先頭に C が付く。STOCKCODE は5桁の数字が多く、末尾に英字が付くものもある'
)) AS p;

-- STEP 11-2  渡す統計の中身を確かめる
-- 【読み方】4KB 前後の JSON。個人を特定する値（顧客 ID の個別の値・名前）は含まない。渡すのは「割合」と「分位点」
SELECT LENGTH(p) AS "文字数", LEFT(p, 400) AS "先頭" FROM FROSTY_DB.W120_LLM.PROFILE;

-- STEP 11-3  使うモデルを決める
-- 【ねらい】2026-11 に、同じプロンプト・同じ統計・各 6 本で、claude-sonnet-4-5 と claude-sonnet-5-5 を比べた。5.5 のほうが、元に近いデータを作る SQL を書いた
--   （STEP 12-3 の×の数：4.5 は 4 表すべて 8、5.5 は 6 表で 1〜3）。そのため 5.5 を使う
--   参考：claude-opus-5-5 を同じ条件（107 万行指定・A と B 各 3 本）で流した結果は、6 本とも動き、「同じ顧客は同じ国」は A でも守られた（5.5 の A は 0.42〜0.43）。
--   ただし×の数は A が 3／1／1、B が 2／1／1 で、5.5 の B（1）より減らず、崩れる項目が変わる（商品コードの種類・月別の形など）。1 本の生成に約 4〜5 分かかり、出力は約 1 万トークン。ハンズオンの時間内では 5.5 を使う
-- 【注意】5.5 は公開プレビューで、1 回の呼び出しに 1〜3 分かかる。使えるモデルは、アカウントの地域設定と、ロールの権限で変わる
--   （検証に使ったアカウントでは claude-fable-5-1 だけ、権限のエラーで使えなかった）。5.5 が使えなければ、claude-sonnet-4-5 に替える
SET LLM = 'claude-sonnet-5-5';
-- 【実行時間の上限】11-4 の生成は、6 本をまとめて約 2〜4 分かかる。上限が 180 秒のアカウント（検証に使ったアカウントの新しい画面）では、途中で取り消される（2026-11-11 に確認）。
--   生成の前に上限を 30 分に上げておく（11-6 で、SQL の実行用に 180 秒へ下げ直す）。この 1 文も流してから 11-4 に進む
ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 1800;

-- STEP 11-4  LLM に SQL を 6 本書かせる（A × 3・B × 3）
-- 【注意】max_tokens は 32,000 にする。claude-sonnet-5-5 は、8,000 だと回答の前に上限に達して、空の応答（NULL や空文字）が返る（2026-11 の実測）
-- 【待つ間に】約 2〜3 分。A と B の違いを話す：A は統計だけ。B は統計に、6 つの業務ルール
--   （金額＝数量×単価／C ⇔ マイナス／同じ請求書は同じ日時・顧客・国／同じ商品コードは同じ商品名／同じ顧客は同じ国／営業は 7〜20 時で土曜は休み）を文章で足す
CREATE OR REPLACE TABLE FROSTY_DB.W120_LLM.GEN AS
WITH v AS (SELECT 'A' AS variant, seq4() AS k FROM TABLE(GENERATOR(ROWCOUNT => 3))
           UNION ALL SELECT 'B', seq4() FROM TABLE(GENERATOR(ROWCOUNT => 3)))
SELECT v.variant, v.k, AI_COMPLETE(
  model => $LLM,
  prompt => $$あなたは Snowflake の SQL でテストデータを作る専門家です。英国のネット通販の取引明細を1067371行作る SQL を1文で書いてください。条件：文は必ず CREATE OR REPLACE TABLE <表名> AS で始める。その後ろは WITH や SELECT を使ってよい。FROM TABLE(GENERATOR(ROWCOUNT => 1067371)) を起点にし、ほかの表は参照しない。セミコロンは付けない。列は次の9列にする。INVOICE VARCHAR、STOCKCODE VARCHAR、DESCRIPTION VARCHAR、QUANTITY NUMBER、INVOICEDATE TIMESTAMP_NTZ、PRICE NUMBER(12,2)、CUSTOMER_ID NUMBER、COUNTRY VARCHAR、AMOUNT NUMBER(14,2)。SQL 以外の説明は書かない。$$
    || ' 表名は FROSTY_DB.W120_LLM.' || v.variant || '_' || (v.k + 1) || ' とする。'
    || IFF(v.variant = 'B',
         $$ 次の業務ルールをすべて満たすこと。1. AMOUNT は ROUND(QUANTITY * PRICE, 2) にする。2. INVOICE が C で始まる行（キャンセル）は QUANTITY を負の数にし、C で始まらない行は QUANTITY を正の数にする（例外は全体の0.3%程度でよい）。3. 同じ INVOICE の行は、INVOICEDATE（同一の日時）・CUSTOMER_ID・COUNTRY をすべて同じにする。1つの請求書は複数の行を持つ。4. 同じ STOCKCODE の行は DESCRIPTION を同じにする（商品名は実在でなくてよく、STOCKCODE から機械的に作ってよい）。5. 同じ CUSTOMER_ID の行は COUNTRY を同じにする。6. 土曜日はほぼ営業しない。営業は7時から20時までで、日曜は営業する。$$, '')
    || ' 元データの統計（JSON）：' || (SELECT p FROM FROSTY_DB.W120_LLM.PROFILE),
  model_parameters => {'temperature': 0.7, 'max_tokens': 32000},
  response_format => {'type': 'json', 'schema': {'type': 'object', 'properties': {'sql': {'type': 'string'}}, 'required': ['sql']}}
) AS resp
FROM v;

-- STEP 11-5 書かれた SQL の長さと書き出し
-- 【読み方】6 本とも、4〜9KB 前後の SQL。書き出しが「CREATE OR REPLACE TABLE FROSTY_DB.W120_LLM.A_1 AS」のように、指定した表名で始まっているか
SELECT variant, k, LENGTH(TRY_PARSE_JSON(resp::STRING):sql::STRING) AS "SQLの長さ",
       LEFT(TRY_PARSE_JSON(resp::STRING):sql::STRING, 90) AS "書き出し"
FROM FROSTY_DB.W120_LLM.GEN ORDER BY variant, k;

-- STEP 11-6  実行結果の記録表を作り、1 文あたりの実行時間の上限を決める
-- 【ねらい】LLM が書いた SQL には、重すぎて終わらないものがある（2026-11 の再実行では、B_3 を直させた SQL が 7 分以上走り続けた）。
--   上限を 180 秒にして、超えたものは error として記録して、先へ進む。ここまでの文は数秒〜2 分なので、上限には掛からない
-- 【流し方】2 文をまとめて選択して Ctrl+Enter
CREATE OR REPLACE TABLE FROSTY_DB.W120_LLM.RUNLOG (round NUMBER, tbl STRING, status STRING, msg STRING);

ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 180;

-- STEP 11-7  検査を通った SQL だけ実行し、結果を記録する（1 回目）
-- 【ねらい】LLM が書いた SQL は、そのまま流さない。決めた表を作る 1 文で始まり、セミコロンや DROP・DELETE などを含まないものだけ実行する
-- 【待つ間に】約 1 分。6 本の SQL を順に実行している
EXECUTE IMMEDIATE $$
DECLARE
  c1 CURSOR FOR SELECT variant, k, RTRIM(TRIM(TRY_PARSE_JSON(resp::STRING):sql::STRING), ';') AS s FROM FROSTY_DB.W120_LLM.GEN ORDER BY variant, k;
  tgt STRING;
  err STRING;
BEGIN
  FOR rec IN c1 DO
    tgt := 'FROSTY_DB.W120_LLM.' || rec.variant || '_' || (rec.k + 1);
    IF (rec.s IS NULL
        OR NOT STARTSWITH(UPPER(rec.s), 'CREATE OR REPLACE TABLE ' || tgt)
        OR CONTAINS(rec.s, ';')
        OR REGEXP_LIKE(REGEXP_REPLACE(UPPER(rec.s), '\\b(GET|INSERT|PUT)\\s*\\(', 'FN('), '.*\\b(DROP|DELETE|UPDATE|INSERT|MERGE|GRANT|REVOKE|ALTER|TRUNCATE|CALL|COPY|PUT|GET)\\b.*', 's')) THEN
      INSERT INTO FROSTY_DB.W120_LLM.RUNLOG VALUES (1, :tgt, 'rejected', NULL);
    ELSE
      BEGIN
        EXECUTE IMMEDIATE rec.s;
        INSERT INTO FROSTY_DB.W120_LLM.RUNLOG VALUES (1, :tgt, 'ok', NULL);
      EXCEPTION WHEN OTHER THEN
        err := LEFT(SQLERRM, 300);
        INSERT INTO FROSTY_DB.W120_LLM.RUNLOG VALUES (1, :tgt, 'error', :err);
      END;
    END IF;
  END FOR;
  RETURN 'done';
END;
$$;

-- STEP 11-8 1 回目に動いたのは何本か
-- 【読み方】status が ok の本数と、error の文を見る
-- 【期待する結果】実行のたびに変わる。2026-11 の claude-sonnet-5-5（検証に使ったアカウント）は、6 本中 6 本が ok。
--   参考：claude-sonnet-4-5 は、6 本中 1 本（最初の検証）と 4 本（再実行）が ok。error の文は、次の 2 種類だった：
--   「argument 1 to function RANDOM needs to be constant, found '...'」（RANDOM の引数に、行ごとに変わる値を渡している）
--   「SQL compilation error: syntax error line 1 ...」（書かれた SQL の構文が誤っている）
-- 【言えること】モデルによって、そのまま動く SQL の本数が違う（4.5 は 1〜4 本、5.5 は 6 本）。全部動いた場合、次の STEP 11-9〜11-11 は、直す対象が無い（11-9 は 0 行を作り、11-10 も何もしない）
SELECT round, tbl, status, msg FROM FROSTY_DB.W120_LLM.RUNLOG WHERE round = 1 ORDER BY tbl;

-- STEP 11-9  エラー文を LLM に返して、1 回だけ直させる
-- 【待つ間に】約 3〜5 分。動かなかった SQL と、そのエラー文を渡して、直した SQL を書かせる
CREATE OR REPLACE TABLE FROSTY_DB.W120_LLM.GEN2 AS
SELECT g.variant, g.k, AI_COMPLETE(
  model => $LLM,
  prompt => $$次の Snowflake の SQL を実行したところ、エラーになりました。エラー文を読んで、表名・列・仕様は変えずに、エラーが出ないよう直した SQL を1文で書いてください。セミコロンは付けず、SQL 以外の説明は書かないでください。$$
    || ' エラー：' || r.msg || ' SQL：' || TRY_PARSE_JSON(g.resp::STRING):sql::STRING,
  model_parameters => {'temperature': 0.2, 'max_tokens': 32000},
  response_format => {'type': 'json', 'schema': {'type': 'object', 'properties': {'sql': {'type': 'string'}}, 'required': ['sql']}}
) AS resp
FROM FROSTY_DB.W120_LLM.GEN g
JOIN FROSTY_DB.W120_LLM.RUNLOG r ON r.round = 1 AND r.tbl = 'FROSTY_DB.W120_LLM.' || g.variant || '_' || (g.k + 1)
WHERE r.status <> 'ok';

-- STEP 11-10  直した SQL を、検査を通して実行する（2 回目）
EXECUTE IMMEDIATE $$
DECLARE
  c1 CURSOR FOR SELECT variant, k, RTRIM(TRIM(TRY_PARSE_JSON(resp::STRING):sql::STRING), ';') AS s FROM FROSTY_DB.W120_LLM.GEN2 ORDER BY variant, k;
  tgt STRING;
  err STRING;
BEGIN
  FOR rec IN c1 DO
    tgt := 'FROSTY_DB.W120_LLM.' || rec.variant || '_' || (rec.k + 1);
    IF (rec.s IS NULL
        OR NOT STARTSWITH(UPPER(rec.s), 'CREATE OR REPLACE TABLE ' || tgt)
        OR CONTAINS(rec.s, ';')
        OR REGEXP_LIKE(REGEXP_REPLACE(UPPER(rec.s), '\\b(GET|INSERT|PUT)\\s*\\(', 'FN('), '.*\\b(DROP|DELETE|UPDATE|INSERT|MERGE|GRANT|REVOKE|ALTER|TRUNCATE|CALL|COPY|PUT|GET)\\b.*', 's')) THEN
      INSERT INTO FROSTY_DB.W120_LLM.RUNLOG VALUES (2, :tgt, 'rejected', NULL);
    ELSE
      BEGIN
        EXECUTE IMMEDIATE rec.s;
        INSERT INTO FROSTY_DB.W120_LLM.RUNLOG VALUES (2, :tgt, 'ok', NULL);
      EXCEPTION WHEN OTHER THEN
        err := LEFT(SQLERRM, 300);
        INSERT INTO FROSTY_DB.W120_LLM.RUNLOG VALUES (2, :tgt, 'error', :err);
      END;
    END IF;
  END FOR;
  RETURN 'done';
END;
$$;

-- STEP 11-11  直させた後は何本動いたか
-- 【読み方】2 回目の ok の本数。動いても、中身が破綻していないかは、次の比較（行数・顧客の種類）で分かる
-- 【期待する結果】5.5 は 1 回目で全部動くため、2 回目の行は無い（6 行のまま）。
--   参考：4.5 は、最初の検証で 2 回目に 5 本が ok（A 3 本・B 2 本）。再実行では B_2 に同じ構文エラーが残り、B_3 は直させた SQL が 7 分以上終わらず、手動で止めて「SQL execution canceled」になった（当時は上限を付けていなかった）
-- 【言えること】エラー文を返して直させると、動く本数が増えることがある。増えないこともあり、直した SQL が重くて終わらないこともある（11-6 の上限は、その備え）
SELECT round, tbl, status, msg FROM FROSTY_DB.W120_LLM.RUNLOG ORDER BY tbl, round;

-- STEP 11-12 動いた表を、同じ測定SQLで測って、比較表に足す
-- 【ねらい】RUNLOG で ok になった表を順に、V_CHECK の向け先にして測り、比較表に 1 行ずつ足す（表の数だけ繰り返す）
-- 【読み方】表名が「LLM A_1」のように行名になる。A は統計だけ、B は統計＋業務ルール
-- 【期待する結果（2026-11・検証に使ったアカウント・claude-sonnet-5-5 に 1,067,371 行を指定した 6 表）】
--   行数：6 表とも 1,067,371（指定どおり）。金額＝数量×単価：6 表とも 1。請求書あたりの行数：19.9〜20.0（元 19.9）。月別の形：0.996〜0.999（元 1、GSD は 0.99）
--   国が複数ある顧客：A（統計だけ）は 3 表とも 0.42〜0.43（元 0.0022）、B（業務ルールの文章つき）は 3 表とも 0
--   商品名が複数ある商品コード：6 表とも 0〜0.0002（元 0.23。名前の揺れは再現しない）
-- 参考（同じ SQL の作り直しや、別の指定のとき）：
--   ・5.5 に 10 万行を指定して書かせた 6 表：国が複数ある顧客が A で 0.05（請求書が 5,000 件で、顧客あたり 1.4 件のため、ずれが見えなかった）。4 表は、行数を 107 万に替えると請求書あたりの行数が 205〜213 になった（請求書数が行数に連動しない SQL）。月別の形が 0.26 の表が 1 つ
--   ・claude-sonnet-4-5 に 10 万行を指定した 4 表：金額が 1 でない表が 2 つ（0.001）、月別の形 0.25、営業時間外 0〜0.42、日時が複数ある請求書 0.05〜1、国が複数ある顧客 0〜0.66 と、表ごとに大きくばらついた
EXECUTE IMMEDIATE $$
DECLARE
  c1 CURSOR FOR SELECT DISTINCT tbl FROM FROSTY_DB.W120_LLM.RUNLOG WHERE status = 'ok' ORDER BY tbl;
  q1 STRING;
  q2 STRING;
  lbl STRING;
BEGIN
  -- 【やり直し】先に「LLM」で始まる行をすべて消すので、何度流しても重複しない
  EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE FROSTY_DB.W120_UCI.SCOREBOARD AS SELECT * FROM FROSTY_DB.W120_UCI.SCOREBOARD WHERE "方法" NOT LIKE ''LLM %''';
  FOR r IN c1 DO
    lbl := 'LLM ' || SPLIT_PART(r.tbl, '.', 3);
    q1 := 'CREATE OR REPLACE VIEW FROSTY_DB.W120_UCI.V_CHECK AS SELECT * FROM ' || r.tbl;
    EXECUTE IMMEDIATE q1;
    q2 := 'CREATE OR REPLACE TABLE FROSTY_DB.W120_UCI.SCOREBOARD AS SELECT * FROM FROSTY_DB.W120_UCI.SCOREBOARD UNION ALL SELECT ''' || lbl || ''', * FROM FROSTY_DB.W120_UCI.V_METRICS';
    EXECUTE IMMEDIATE q2;
  END FOR;
  RETURN 'done';
END;
$$;

-- STEP 11-13  LLM の表のうち、元と別物になっているものを見つける
-- 【読み方】10 万行を作らせたのに行数が大きく違う、1 請求書あたりの行数が元（19.9）から桁違い、商品コードの種類が元（5,304）から桁違い、などは、
--   動いても元に似たデータになっていない
-- 【期待する結果（2026-11・5.5・1,067,371 行を指定）】6 表とも 1,067,371 行。請求書あたりの行数は 19.9〜20.0。商品コードの種類は、判定表の許容（元の ±10%）以内。破綻した表は無い
--   参考（4.5・10 万行を指定）：A_1 の商品コードが 4.8 万種類、A_3 の請求書あたりが 12,500 行、B_1 の行数が 85.5 万行と、元と別物の表が 3 つあった
-- 【言えること】「動いた」と「使える」は別。4.5 は、動いても元と別物になった表が多かった。5.5 は、6 表とも元と同じ規模・同じ構造の表になった
SELECT "方法", "行数", "請求書あたりの行数", "商品コードの種類", "国が複数ある顧客"
FROM FROSTY_DB.W120_UCI.SCOREBOARD WHERE "方法" LIKE 'LLM %' ORDER BY "方法";


-- ============================================================================
-- STEP 12  比較表と判定表（ここから結論を読む）
-- ============================================================================

-- STEP 12-1 比較表（行＝方法、列＝11 項目と行数）
-- 【読み方】元データの行と比べて、大きく外れた値を見る（2026-11 の実測）
--   ① 月別の形（1 列ずつの分布）：GSD は自動で残す（0.99）。LLM は、5.5 では多くの表で残し（0.98 前後）、4.5 では残せなかった（0.25）
--   ② 金額・請求書ごとの日時・商品名・キャンセルの規則：GSD は 1 表のままでは崩れ、表を分けて書いたときだけ守られた。LLM は、5.5 では SQL の書き方（請求書ごとに乱数を作る）で守られ、4.5 では崩れた。
--      「同じ顧客は同じ国」は、業務ルールを文章で書いた B だけ守られ、統計だけの A は 3 本とも守られなかった（1,067,371 行を指定した 5.5）
--   ③ 営業時間外：GSD はどの方法でも直らない。LLM の 5.5 は、6 表とも 0 だった
--   ④ 直したつもりの方法が、別の項目を壊すことがある（GSD を 3 表に分けると、キャンセル行・顧客と国が崩れる）
SELECT * FROM FROSTY_DB.W120_UCI.SCOREBOARD
ORDER BY CASE WHEN "方法" = '元データ' THEN 0 WHEN "方法" = 'GSD 1表のまま' THEN 1 WHEN "方法" = 'GSD 3表に分割' THEN 2 ELSE 3 END, "方法";

-- STEP 12-2  判定表のビューを作る：元データから外れた項目に × を付ける（○＝元に近い）
-- 【× の意味】「元データと違う」であり、「悪い」ではない。たとえば「商品名が複数ある商品コード」は、元データ自体が 22.9% で
--   名前が揺れている。3 表に分けると 1 コード 1 名に揃って 0 になり、元より整う。分析で「元の揺れ」を再現したいのでなければ、
--   これは × でも問題にならない。× を見たら、「元と違うことが、自分の分析で困るか」を考える
-- 【判定の基準】元データの値との差が、次の許容を超えたら ×
--   割合の項目：0.05（キャンセル行・土曜は 0.01、C⇔マイナス数量は 0.02。値が小さい項目なので厳しくする）
--   月別の形（相関）：0.1 ／ 請求書あたりの行数：元の ±30% ／ 商品コードの種類：元の ±10%
--   ※ 基準は、この比較のために決めた目安。分析の目的によって、許容は変わる
CREATE OR REPLACE VIEW FROSTY_DB.W120_UCI.V_JUDGE AS
WITH b AS (SELECT * FROM FROSTY_DB.W120_UCI.SCOREBOARD WHERE "方法" = '元データ'),
j AS (
  SELECT s."方法",
    IFF(ABS(s."金額＝数量×単価" - b."金額＝数量×単価") > 0.05, '×', '○') AS "金額＝数量×単価",
    IFF(ABS(s."日時が複数ある請求書" - b."日時が複数ある請求書") > 0.05, '×', '○') AS "日時が複数ある請求書",
    IFF(ABS(s."商品名が複数ある商品コード" - b."商品名が複数ある商品コード") > 0.05, '×', '○') AS "商品名が複数ある商品コード",
    IFF(ABS(s."国が複数ある顧客" - b."国が複数ある顧客") > 0.05, '×', '○') AS "国が複数ある顧客",
    IFF(ABS(s."キャンセル行の割合" - b."キャンセル行の割合") > 0.01, '×', '○') AS "キャンセル行の割合",
    IFF(ABS(s."C⇔マイナス数量" - b."C⇔マイナス数量") > 0.02, '×', '○') AS "C⇔マイナス数量",
    IFF(ABS(s."月別の件数の形（元との相関）" - b."月別の件数の形（元との相関）") > 0.1, '×', '○') AS "月別の件数の形",
    IFF(ABS(s."営業時間外の割合" - b."営業時間外の割合") > 0.05, '×', '○') AS "営業時間外の割合",
    IFF(ABS(s."土曜の割合" - b."土曜の割合") > 0.01, '×', '○') AS "土曜の割合",
    IFF(ABS(s."請求書あたりの行数" / b."請求書あたりの行数" - 1) > 0.3, '×', '○') AS "請求書あたりの行数",
    IFF(ABS(s."商品コードの種類" / b."商品コードの種類" - 1) > 0.1, '×', '○') AS "商品コードの種類"
  FROM FROSTY_DB.W120_UCI.SCOREBOARD s CROSS JOIN b)
SELECT j.*, (IFF("金額＝数量×単価" = '×', 1, 0) + IFF("日時が複数ある請求書" = '×', 1, 0) + IFF("商品名が複数ある商品コード" = '×', 1, 0)
  + IFF("国が複数ある顧客" = '×', 1, 0) + IFF("キャンセル行の割合" = '×', 1, 0) + IFF("C⇔マイナス数量" = '×', 1, 0)
  + IFF("月別の件数の形" = '×', 1, 0) + IFF("営業時間外の割合" = '×', 1, 0) + IFF("土曜の割合" = '×', 1, 0)
  + IFF("請求書あたりの行数" = '×', 1, 0) + IFF("商品コードの種類" = '×', 1, 0)) AS "×の数（11項目中）"
FROM j;

-- STEP 12-3 判定表を見る
-- 【読み方】×は元と違う項目の数で、優劣ではない。×の数だけで選ばない。
--   自分の分析で使う項目（たとえば返品率なら「キャンセル行の割合」）が ○ かを見る。×の項目で分析すると、結論が元と変わる
-- 【期待する結果】元データ 0 ／ GSD 3 表に分割 6 ／ GSD 1 表のまま 8。LLM（2026-11・検証に使ったアカウント）：claude-sonnet-5-5 に 1,067,371 行を指定した 6 表は 1〜2
--   （A の 3 表が 2＝商品名・国、B の 3 表が 1＝商品名）。参考：5.5 に 10 万行を指定した 6 表は 1〜3、その SQL の行数だけを 107 万に替えると 2〜3。claude-sonnet-4-5（10 万行指定）の 4 表は、すべて 8。
--   GSD と LLM は同じ Enterprise アカウントで流して、同じ表に並べる
-- 【言えること】5.5 の LLM は、GSD（6〜8）より×が少ない（1〜2）。ただし、LLM に渡した統計の項目（月別・時間帯・土曜・キャンセル率・行数・種類数）は、渡した値の再現に近く、前処理の分だけ LLM に有利なので、方法の優劣は決めない。残る×は、商品名（元の揺れ 0.23 が 0 に揃う。問題にならない場合が多い）と、A の国だけ。
--   A（統計だけ）と B（業務ルールの文章つき）の差は、「同じ顧客は同じ国」の 1 項目に出た。A は国を顧客と別に抽選し（3 本とも 0.42〜0.43）、B は文章どおり守った（3 本とも 0）。業務ルールは、書かないと守られない。
--   5.5 が書いた SQL を読んだ（A_1・B_2。10 万行を指定したとき）。分かったこと：
--     ・同じ請求書は同じ日時・同じ商品コードは同じ商品名：構造で成立する作り。20 行ずつを 1 請求書にし、請求書ごとの乱数（HASH）から日時・顧客・国を決め、商品名は商品コードの HASH で決める。統計を使いこなしたのではなく、データ生成の定石どおりに書いている
--     ・月別の形：統計の月別の割合を累積の閾値にして使う。10 万行のときの B_2 は、日付を全期間から一様に抽選して 0.26 だった（1,067,371 行を指定した 6 表は、すべて 0.996 以上）
--     ・土曜は、土曜になった日付を金曜にずらして 0 にする。営業時間外は、統計の時間帯別の割合を使うため 0
--     ・請求書数：10 万行を指定した SQL は、行数を替えると請求書数が連動しないものがあった。最初から 1,067,371 行を指定すると、6 本とも連動した（請求書あたり 19.9〜20.0）
--   どちらの方法も、元の「揺れ」や「例外」（商品名の揺れ、C ⇔ マイナスの例外）は再現しない
--   参考（claude-opus-5-5 を同じ条件で流した 6 表）：×は A が 3／1／1、B が 2／1／1。国が複数ある顧客は A でも 3 本とも 0（5.5 の A は 0.42〜0.43）。
--   ただし、A_1 は商品コードの種類 4,700・C ⇔ マイナス 0.9566、B_1 は月別の形 0.866 が×。モデルを上げると、崩れる場所が変わる（×の数は減らない）。
--   Opus の SQL は、顧客をキーにした HASH（顧客から国を決める）が入っていた。5.5 は請求書だけがキー
SELECT * FROM FROSTY_DB.W120_UCI.V_JUDGE
ORDER BY CASE WHEN "方法" = '元データ' THEN 0 WHEN "方法" = 'GSD 1表のまま' THEN 1 WHEN "方法" = 'GSD 3表に分割' THEN 2 ELSE 3 END, "方法";

-- STEP 12-4 目的ごとの合否：使う分析に必要な項目だけを見る
-- 【ねらい】×の数だけで選ぶと、使う分析で必要な項目が崩れていても、他の項目で埋め合わせて基準内に見える。分析の目的ごとに、必須の項目を決めて、すべて ○ なら基準内とする
-- 【判定の決め方】返品率の分析＝キャンセル行の割合・C⇔マイナス数量・金額の 3 項目 ／ 顧客と国の分析＝国が複数ある顧客 ／ 月別の動きの分析＝月別の件数の形
--   ※ 自分の分析に替えるときは、IFF の中の項目を替える。必須でない項目は、別に割合で見る（たとえば 11 項目中 ×が 3〜4 個以内）
-- 【期待する結果】GSD 1 表は、返品率と顧客と国が×あり、月別の動きは基準内（月別の形 0.991）。GSD 3 表は、3 つの目的とも×あり（月別の形 0.84）。5.5 の A は、返品率・月別の動きは基準内、顧客と国は×あり（0.42〜0.43）。5.5 の B と Opus の A・B は、顧客と国は基準内
--   Opus は、A_1 が返品率で×あり（C⇔マイナス）、B_1 が月別の動きで×あり
-- 【言えること】同じ表でも、「返品率には使える、顧客と国の分析には使えない」と、目的ごとに答えが変わる。基準内の線は、データを使う人が決める
SELECT "方法",
  IFF("キャンセル行の割合" = '○' AND "C⇔マイナス数量" = '○' AND "金額＝数量×単価" = '○', '基準内', '×あり') AS "返品率の分析",
  IFF("国が複数ある顧客" = '○', '基準内', '×あり') AS "顧客と国の分析",
  IFF("月別の件数の形" = '○', '基準内', '×あり') AS "月別の動きの分析",
  "×の数（11項目中）"
FROM FROSTY_DB.W120_UCI.V_JUDGE
ORDER BY CASE WHEN "方法" = '元データ' THEN 0 WHEN "方法" = 'GSD 1表のまま' THEN 1 WHEN "方法" = 'GSD 3表に分割' THEN 2 ELSE 3 END, "方法";


-- ============================================================================
-- STEP 13  自分のデータで、合成する前に数える 4 つ
-- ============================================================================
-- ここでは、UCI の元データ（FROSTY_DB.W120_UCI.SRC）で流す。自分のデータで使うときは、表名と列名を替える。
-- 合成の前に、元データのどの関係が成り立っているかを数える。成り立っている関係は、合成の前に、表の分け方や列の置き換えで書く。
-- 書いていない関係は、STEP 12 の比較表のとおり、合成後には残らない

-- STEP 13-1  式が成り立つ行の割合（例：金額＝数量×単価）
-- 【読み方】1 に近ければ、成り立っている式。合成の前に、列の置き換えか、組み立てで書く
-- 【期待する結果】1.0
SELECT ROUND(AVG(IFF(ABS(AMOUNT - QUANTITY * PRICE) < 0.015, 1, 0)), 4) AS "式が成り立つ行の割合" FROM FROSTY_DB.W120_UCI.SRC;

-- STEP 13-2  同じ請求番号の行が、日時を何種類持つか
-- 【読み方】1 種類の請求書がほとんどなら、「請求書単位の表」に分けて合成する
-- 【期待する結果】2 種類以上の請求書は 0.15%
SELECT COUNT(*) AS "請求書の数", COUNT_IF(c > 1) AS "日時が2種類以上の請求書", ROUND(COUNT_IF(c > 1) / COUNT(*) * 100, 2) AS "割合（％）"
FROM (SELECT INVOICE, COUNT(DISTINCT INVOICEDATE) AS c FROM FROSTY_DB.W120_UCI.SRC GROUP BY INVOICE);

-- STEP 13-3  同じ商品コードが、商品名を何種類持つか
-- 【読み方】1 種類の商品コードがほとんどなら、「商品の表」に分けて合成する。揺れが多いなら、先に商品名を整える
-- 【期待する結果】2 種類以上の商品コードは 22.9%
SELECT COUNT(*) AS "商品コードの数", COUNT_IF(c > 1) AS "商品名が2種類以上", ROUND(COUNT_IF(c > 1) / COUNT(*) * 100, 1) AS "割合（％）"
FROM (SELECT STOCKCODE, COUNT(DISTINCT DESCRIPTION) AS c FROM FROSTY_DB.W120_UCI.SRC GROUP BY STOCKCODE);

-- STEP 13-4  時間帯別・曜日別の件数
-- 【読み方】営業しない時間帯・曜日が、はっきり 0 に近いなら、合成後に埋まる（STEP 9-3・9-4）。
--   時刻や曜日で分析するなら、元の分布を別に持つか、その項目を入力で渡す
-- 【期待する結果】7 時前・21 時以降は 42 行だけ（全体の 0.004%）/ 土曜 402 行
SELECT HOUR(INVOICEDATE) AS "時", DAYNAME(INVOICEDATE) AS "曜日", COUNT(*) AS "件数"
FROM FROSTY_DB.W120_UCI.SRC GROUP BY 1, 2 ORDER BY 1, 2;


-- ============================================================================
-- 後片付け（ハンズオンが終わってから）
-- ============================================================================
-- 次の 2 文を流すと、この回で作ったものがすべて消える。他の回のオブジェクトが FROSTY_DB にある場合は、
-- DROP DATABASE ではなく、W120_* と WEEK_120 のスキーマを 1 つずつ DROP する
-- DROP DATABASE IF EXISTS FROSTY_DB;
-- DROP WAREHOUSE IF EXISTS FF_W120_WH;


-- ============================================================================
-- STEP 14  【任意・当日は流さない】LLM の傾向が、繰り返しても変わらないか確かめる
-- ============================================================================
-- 同じプロンプト・同じ統計で、SQL を何ラウンドも書かせ、同じ測定SQLで測って、×の出方が毎回同じかを見る
-- 【前提】STEP 11-1（PROFILE）と STEP 8-10〜8-12（測定SQLと元データの行）が済んでいること。生成は約 3〜4 分（6 本）。クレジットを使う
-- 【結果の例】2026-11-11、検証に使ったアカウント、各モデル・各条件（A・B）9 本（3 ラウンド × 3 本）。

-- STEP 14-1  入れ物を作る
-- 【読み方】GEN_R＝生成した SQL（モデル・ラウンド・条件・番号つき）、EXEC_LOG＝実行の記録
CREATE SCHEMA IF NOT EXISTS FROSTY_DB.W120_REP;
CREATE TABLE IF NOT EXISTS FROSTY_DB.W120_REP.GEN_R (model STRING, rnd INT, variant STRING, k INT, resp VARIANT);
CREATE TABLE IF NOT EXISTS FROSTY_DB.W120_REP.EXEC_LOG (lbl STRING, msg STRING);

-- STEP 14-2  生成の上限時間を上げる
-- 【読み方】6 本の生成は 2〜4 分。上限が 180 秒のままだと、途中で取り消される
ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 3600;

-- STEP 14-3  ラウンド 2 を生成する（A × 3・B × 3）。モデルを替えるときは、model と表名の S55 を替える（Opus は claude-opus-5-5 と OPUS）
-- 【待つ間に】約 3〜4 分。1 ラウンドずつ流す（12 本をまとめると、時間が読めない）。ラウンドを増やすときは、SELECT 2 AS rnd の 2 を 3 に替える
INSERT INTO FROSTY_DB.W120_REP.GEN_R
SELECT 'claude-sonnet-5-5', v.rnd, v.variant, v.k, AI_COMPLETE(
  model => 'claude-sonnet-5-5',
  prompt => $$あなたは Snowflake の SQL でテストデータを作る専門家です。英国のネット通販の取引明細を1067371行作る SQL を1文で書いてください。条件：文は必ず CREATE OR REPLACE TABLE <表名> AS で始める。その後ろは WITH や SELECT を使ってよい。FROM TABLE(GENERATOR(ROWCOUNT => 1067371)) を起点にし、ほかの表は参照しない。セミコロンは付けない。列は次の9列にする。INVOICE VARCHAR、STOCKCODE VARCHAR、DESCRIPTION VARCHAR、QUANTITY NUMBER、INVOICEDATE TIMESTAMP_NTZ、PRICE NUMBER(12,2)、CUSTOMER_ID NUMBER、COUNTRY VARCHAR、AMOUNT NUMBER(14,2)。SQL 以外の説明は書かない。$$
    || ' 表名は FROSTY_DB.W120_REP.S55_r' || v.rnd || '_' || v.variant || (v.k + 1) || ' とする。'
    || IFF(v.variant = 'B',
         $$ 次の業務ルールをすべて満たすこと。1. AMOUNT は ROUND(QUANTITY * PRICE, 2) にする。2. INVOICE が C で始まる行（キャンセル）は QUANTITY を負の数にし、C で始まらない行は QUANTITY を正の数にする（例外は全体の0.3%程度でよい）。3. 同じ INVOICE の行は、INVOICEDATE（同一の日時）・CUSTOMER_ID・COUNTRY をすべて同じにする。1つの請求書は複数の行を持つ。4. 同じ STOCKCODE の行は DESCRIPTION を同じにする（商品名は実在でなくてよく、STOCKCODE から機械的に作ってよい）。5. 同じ CUSTOMER_ID の行は COUNTRY を同じにする。6. 土曜日はほぼ営業しない。営業は7時から20時までで、日曜は営業する。$$, '')
    || ' 元データの統計（JSON）：' || (SELECT p FROM FROSTY_DB.W120_LLM.PROFILE),
  model_parameters => {'temperature': 0.7, 'max_tokens': 32000},
  response_format => {'type': 'json', 'schema': {'type': 'object', 'properties': {'sql': {'type': 'string'}}, 'required': ['sql']}},
  show_details => TRUE)
FROM (SELECT 2 AS rnd, x.variant, x.k
      FROM (SELECT 'A' AS variant, 0 AS k UNION ALL SELECT 'A', 1 UNION ALL SELECT 'A', 2
            UNION ALL SELECT 'B', 0 UNION ALL SELECT 'B', 1 UNION ALL SELECT 'B', 2) x) v;

-- STEP 14-4  書かれた SQL を、安全確認を通して実行し、測って、比較表に「R:」で始まる行として足す
-- 【読み方】STEP 11-7 と同じ安全確認（CREATE OR REPLACE TABLE <指定の表名> で始まる／セミコロンなし／DROP・DELETE などを含まない）。
--   実行ずみ（EXEC_LOG に OK がある）の SQL は、飛ばす。表名は、大文字小文字を区別せずに比べる（2026-11-11 に、区別して全部「rejected」になったため直した）
-- 【注意】比較表（SCOREBOARD）に「R:」の行が増える。STEP 12-1 の表がごちゃつくので、集計のあとで STEP 14-6 で退避する
EXECUTE IMMEDIATE $$
DECLARE
  c1 CURSOR FOR SELECT model, rnd, variant, k, RTRIM(TRIM(COALESCE(resp:structured_output[0]:raw_message:sql::STRING, TRY_PARSE_JSON(resp::STRING):sql::STRING)), ';') AS s
    FROM FROSTY_DB.W120_REP.GEN_R
    WHERE NOT EXISTS (SELECT 1 FROM FROSTY_DB.W120_REP.EXEC_LOG e WHERE e.msg = 'OK' AND e.lbl = 'R:' || IFF(model LIKE '%opus%', 'OPUS', 'S55') || ' r' || rnd || ' ' || variant || '_' || (k + 1))
    ORDER BY model, rnd, variant, k;
  tgt STRING; sq STRING; tag STRING; lbl STRING; msg STRING;
BEGIN
  FOR rec IN c1 DO
    tag := IFF(rec.model LIKE '%opus%', 'OPUS', 'S55');
    tgt := 'FROSTY_DB.W120_REP.' || tag || '_r' || rec.rnd || '_' || rec.variant || (rec.k + 1);
    lbl := 'R:' || tag || ' r' || rec.rnd || ' ' || rec.variant || '_' || (rec.k + 1);
    sq := rec.s;
    msg := 'OK';
    BEGIN
      IF (sq IS NULL OR NOT STARTSWITH(UPPER(sq), UPPER('CREATE OR REPLACE TABLE ' || tgt)) OR CONTAINS(sq, ';')
          OR REGEXP_LIKE(REGEXP_REPLACE(UPPER(sq), '\\b(GET|INSERT|PUT)\\s*\\(', 'FN('), '.*\\b(DROP|DELETE|UPDATE|INSERT|MERGE|GRANT|REVOKE|ALTER|TRUNCATE|CALL|COPY|PUT|GET)\\b.*', 's')) THEN
        msg := 'rejected';
      ELSE
        EXECUTE IMMEDIATE :sq;
        EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW FROSTY_DB.W120_UCI.V_CHECK AS SELECT * FROM ' || tgt;
        EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE FROSTY_DB.W120_UCI.SCOREBOARD AS SELECT * FROM FROSTY_DB.W120_UCI.SCOREBOARD WHERE "方法" <> ''' || lbl || ''' UNION ALL SELECT ''' || lbl || ''', * FROM FROSTY_DB.W120_UCI.V_METRICS';
      END IF;
    EXCEPTION WHEN OTHER THEN
      msg := 'ERR:' || LEFT(SQLERRM, 100);
    END;
    INSERT INTO FROSTY_DB.W120_REP.EXEC_LOG VALUES (:lbl, :msg);
  END FOR;
  RETURN 'done';
END;
$$;

-- STEP 14-5  条件ごとに、×の出方をまとめる
-- 【読み方】mdl＝モデル、v＝A（統計だけ）か B（業務ルールつき）、n＝本数、avg_x＝×の項目数の平均、kuni_ok＝「国が複数ある顧客」が基準内の本数、
--   name_ok＝「商品名が複数ある商品コード」、tsuki_ok＝「月別の形」、codes_ok＝「商品コードの種類」、cm_ok＝「C⇔マイナス数量」（いずれも基準内の本数）。同じ条件で毎回基準外になる項目（0/n）は、傾向として安定している
-- 【期待する結果（2026-11-11、各 9 本）】5.5 A は avg 2.11・国 0/9・商品名 0/9 ／ 5.5 B は avg 1.11・国 9/9・商品名 0/9・月別 8/9 ／
--   Opus A は avg 1.33・国 8/9・商品名 0/9 ／ Opus B は avg 1.44・国 9/9・商品名 0/9・月別 5/9
CREATE OR REPLACE VIEW FROSTY_DB.W120_REP.V_STAB AS
SELECT j."方法" AS lbl,
       IFF(j."方法" LIKE 'Opus %' OR j."方法" LIKE 'R:OPUS%', 'Opus', '5.5') AS mdl,
       LEFT(REGEXP_SUBSTR(j."方法", '[AB]_[0-9]'), 1) AS v,
       j."×の数（11項目中）" AS nx, s."国が複数ある顧客" AS kuni, j."金額＝数量×単価" AS j_kin, j."日時が複数ある請求書" AS j_inv,
       j."商品名が複数ある商品コード" AS j_name, j."国が複数ある顧客" AS j_kuni, j."キャンセル行の割合" AS j_can, j."C⇔マイナス数量" AS j_cm,
       j."月別の件数の形" AS j_tsuki, j."営業時間外の割合" AS j_hrs, j."土曜の割合" AS j_sat, j."請求書あたりの行数" AS j_lpi, j."商品コードの種類" AS j_codes
FROM FROSTY_DB.W120_UCI.V_JUDGE j JOIN FROSTY_DB.W120_UCI.SCOREBOARD s ON s."方法" = j."方法"
WHERE REGEXP_SUBSTR(j."方法", '[AB]_[0-9]') IS NOT NULL AND (j."方法" LIKE 'LLM %' OR j."方法" LIKE 'R:%' OR j."方法" LIKE 'Opus %' OR j."方法" LIKE 'S5.5M %');

SELECT mdl, v, COUNT(*) AS n, ROUND(AVG(nx), 2) AS avg_x, MIN(nx) AS min_x, MAX(nx) AS max_x,
       COUNT_IF(j_kuni = '○') AS kuni_ok, COUNT_IF(j_name = '○') AS name_ok, COUNT_IF(j_tsuki = '○') AS tsuki_ok, COUNT_IF(j_codes = '○') AS codes_ok, COUNT_IF(j_cm = '○') AS cm_ok
FROM FROSTY_DB.W120_REP.V_STAB GROUP BY 1, 2 ORDER BY 1, 2;

-- STEP 14-6  集計の元を退避して、比較表を元に戻す
-- 【読み方】STEP 12-1 の比較表に、繰り返しの行が混ざらないようにする。退避先は、SCOREBOARD_REP（測定値）と STAB_SNAPSHOT（集計の元）
CREATE OR REPLACE TABLE FROSTY_DB.W120_REP.STAB_SNAPSHOT AS SELECT * FROM FROSTY_DB.W120_REP.V_STAB;
CREATE OR REPLACE TABLE FROSTY_DB.W120_REP.SCOREBOARD_REP AS SELECT * FROM FROSTY_DB.W120_UCI.SCOREBOARD WHERE "方法" LIKE 'R:%';
CREATE OR REPLACE TABLE FROSTY_DB.W120_UCI.SCOREBOARD AS SELECT * FROM FROSTY_DB.W120_UCI.SCOREBOARD WHERE "方法" NOT LIKE 'R:%';
