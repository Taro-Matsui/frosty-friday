-- ============================================================
-- STEP 1: 共通リソース作成（ACCOUNTADMINで実行 / 初回のみ）
-- ============================================================
USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS FROSTY_FRIDAY;

CREATE WAREHOUSE IF NOT EXISTS FROSTY_FRIDAY_XS
    WAREHOUSE_SIZE      = 'XSMALL'
    AUTO_SUSPEND        = 60
    AUTO_RESUME         = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Frosty Friday Challenge 専用ウェアハウス';

CREATE DATABASE IF NOT EXISTS FROSTY_FRIDAY
    COMMENT = 'Frosty Friday Challenge 専用データベース';

GRANT USAGE ON WAREHOUSE FROSTY_FRIDAY_XS TO ROLE FROSTY_FRIDAY;
GRANT USAGE ON DATABASE  FROSTY_FRIDAY     TO ROLE FROSTY_FRIDAY;
GRANT ROLE FROSTY_FRIDAY TO USER TAROADMIN; -- ← ご自身のユーザー名に変更

CREATE SCHEMA IF NOT EXISTS FROSTY_FRIDAY.WEEK_092;

GRANT USAGE        ON SCHEMA FROSTY_FRIDAY.WEEK_092 TO ROLE FROSTY_FRIDAY;
GRANT CREATE TABLE ON SCHEMA FROSTY_FRIDAY.WEEK_092 TO ROLE FROSTY_FRIDAY;
GRANT CREATE VIEW  ON SCHEMA FROSTY_FRIDAY.WEEK_092 TO ROLE FROSTY_FRIDAY;

-- ============================================================
-- STEP 2: 公式チャレンジ セットアップSQL
-- Source: https://frostyfriday.org/blog/2024/05/03/week-92-easy/
-- ============================================================

-- テーブル作成
CREATE OR REPLACE TABLE fruit_salad (
    fruits VARCHAR(255)
);

-- データ投入（29件 ※strawberry x2, apricot x2 含む）
INSERT INTO fruit_salad (fruits) VALUES
    ('apple'),
    ('apricot'),
    ('banana'),
    ('pineapple'),
    ('oranges'),
    ('kiwi'),
    ('strawberry'),    -- 基準文字列 ① 完全一致
    ('grape'),
    ('watermelon'),
    ('pear'),
    ('peach'),
    ('strawberry'),    -- 基準文字列 ② 完全一致
    ('blueberry'),
    ('mango'),
    ('lemon'),
    ('lime'),
    ('papaya'),
    ('cherry'),
    ('plum'),
    ('fig'),
    ('passion fruit'),
    ('raspberry'),
    ('blackberry'),
    ('nectarine'),
    ('cantaloupe'),
    ('apricot'),       -- apricot は重複あり
    ('tangerine'),
    ('guava'),
    ('dragon fruit');

-- ============================================================
-- STEP 3: セットアップ検証
-- ============================================================

-- ① 件数確認（期待値: 29）
SELECT COUNT(*) AS total_rows FROM fruit_salad;

-- ② 重複データ確認（期待値: strawberry x2, apricot x2）
SELECT fruits, COUNT(*) AS cnt
FROM fruit_salad
GROUP BY fruits
HAVING COUNT(*) > 1
ORDER BY fruits;

-- ============================================================
-- STEP 4: 標準解法（Week 92 正答クエリ）
-- ============================================================
SELECT
    fruits,
    JAROWINKLER_SIMILARITY(fruits, 'strawberry') AS similarity_score
FROM fruit_salad
ORDER BY similarity_score DESC;


-- ============================================================
-- STEP 5: 結果の分析・読み解き
-- ============================================================

-- ① スコア分布をグループ化して傾向を把握
SELECT
    similarity_score,
    COUNT(*)        AS fruit_count,
    LISTAGG(fruits, ', ') WITHIN GROUP (ORDER BY fruits) AS fruit_list
FROM (
    SELECT
        fruits,
        JAROWINKLER_SIMILARITY(fruits, 'strawberry') AS similarity_score
    FROM fruit_salad
)
GROUP BY similarity_score
ORDER BY similarity_score DESC;

-- ② 完全一致（スコア100）の確認
SELECT fruits
FROM fruit_salad
WHERE JAROWINKLER_SIMILARITY(fruits, 'strawberry') = 100;
-- 期待値: strawberry が2件

-- ③ ワースト3確認
SELECT
    fruits,
    JAROWINKLER_SIMILARITY(fruits, 'strawberry') AS similarity_score
FROM fruit_salad
ORDER BY similarity_score ASC
LIMIT 3;


-- ============================================================
-- STEP 6: 別解法 - 複数アルゴリズム横並び比較（LLM拡張版）
-- ============================================================

-- アルゴリズム特性：
--   JAROWINKLER_SIMILARITY : 100に近いほど類似・先頭一致をボーナス加点
--   EDITDISTANCE           : 0に近いほど類似・編集操作数（挿入・削除・置換）
--   SOUNDEX                : 英字のみ有効・発音ベースの4文字コード比較
--   AI_SIMILARITY          : Embeddingベース意味的類似度（-1〜1, 1が最類似）
--   AI_CLASSIFY            : LLMが「似ている」か分類判定
--   AI_COMPLETE (llama)    : llama3.1-8b が類似度を0-100で推定
--   AI_COMPLETE (claude)   : claude-sonnet-4-6 が類似度を0-100で推定
--                            ※ モデル間のスコア差でLLM判定のばらつきを観察

WITH similarity_scores AS (
    SELECT
        fruits,
        JAROWINKLER_SIMILARITY(fruits, 'strawberry')     AS jaro_winkler_score,
        EDITDISTANCE(fruits, 'strawberry')                AS edit_distance,
        SOUNDEX(fruits)                                   AS soundex_code,
        SOUNDEX('strawberry')                             AS soundex_ref,
        IFF(
            SOUNDEX(fruits) = SOUNDEX('strawberry'),
            'MATCH', 'NO MATCH'
        )                                                 AS soundex_result,
        AI_SIMILARITY(fruits, 'strawberry')               AS embedding_similarity,
        AI_CLASSIFY(
            fruits,
            ['similar to strawberry', 'not similar to strawberry']
        ):labels[0]::STRING                               AS llm_classify_result,
        TRIM(AI_COMPLETE(
            'llama3.1-8b',
            'Rate the spelling similarity between "' || fruits || '" and "strawberry" on a scale of 0-100. Reply with ONLY the number.'
        ))                                                AS llm_score_llama,
        TRIM(AI_COMPLETE(
            'claude-sonnet-4-6',
            'Rate the spelling similarity between "' || fruits || '" and "strawberry" on a scale of 0-100. Reply with ONLY the number.'
        ))                                                AS llm_score_claude
    FROM fruit_salad
)
SELECT
    fruits,
    jaro_winkler_score,
    edit_distance,
    soundex_code,
    soundex_ref,
    soundex_result,
    ROUND(embedding_similarity, 4)                        AS embedding_similarity,
    llm_classify_result,
    llm_score_llama,
    llm_score_claude,
    ABS(TRY_TO_NUMBER(llm_score_llama) - TRY_TO_NUMBER(llm_score_claude)) AS llm_score_diff
FROM similarity_scores
ORDER BY jaro_winkler_score DESC;



-- アルゴリズム間でランキングが逆転するケースを確認
WITH scores AS (
    SELECT
        fruits,
        JAROWINKLER_SIMILARITY(fruits, 'strawberry') AS jw_score,
        EDITDISTANCE(fruits, 'strawberry')            AS ed_score,
        RANK() OVER (ORDER BY JAROWINKLER_SIMILARITY(fruits, 'strawberry') DESC) AS jw_rank,
        RANK() OVER (ORDER BY EDITDISTANCE(fruits, 'strawberry') ASC)            AS ed_rank
    FROM fruit_salad
)
SELECT
    fruits,
    jw_score,
    ed_score,
    jw_rank,
    ed_rank,
    ABS(jw_rank - ed_rank) AS rank_diff  -- 差が大きいほどアルゴリズム間で評価が割れている
FROM scores
ORDER BY rank_diff DESC;


-- ============================================================
-- SOUNDEX 検証用：発音同一・綴り異なる名前テーブル
-- ============================================================
CREATE OR REPLACE TABLE soundex_demo (
    id          INT,
    name_input  VARCHAR(255),
    pattern     VARCHAR(100)
);

INSERT INTO soundex_demo VALUES
-- ① 完全一致
 (1,  'Catherine',  '基準（正規スペル）'),

-- ② 先頭文字 C → 変わらない（SOUNDEXがMATCHしやすい）
 (2,  'Catharine',  'a挿入（古典的表記）'),
 (3,  'Cathrine',   'e省略'),
 (4,  'Catheryn',   'ine→yn'),
 (5,  'Cathryn',    'e省略+yn'),
 (6,  'Catherin',   '末尾e省略'),
 (7,  'Caterina',   'イタリア語形'),
 (8,  'Caterin',    'イタリア語形・短縮'),

-- ③ 先頭文字 C→K（発音同じだがSOUNDEXは別コード）
 (9,  'Katherine',  'K表記・正規'),
 (10, 'Kathryn',    'K表記・短縮形'),
 (11, 'Katharine',  'K表記・a挿入'),
 (12, 'Kathryne',   'K表記・e追加'),
 (13, 'Katherin',   'K表記・末尾e省略'),
 (14, 'Katerina',   'K表記・東欧形'),
 (15, 'Katarina',   'K表記・スラブ形'),
 (16, 'Katerin',    'K表記・最短縮'),

-- ④ 発音は近いが綴りが大きく異なる（SOUNDEXの限界を見るケース）
 (17, 'Ekaterina',  '先頭にE付与（ロシア形）'),
 (18, 'Aikaterine', '先頭にAik（ギリシャ形）');


WITH scores AS (
    SELECT
        id,
        name_input,
        pattern,
        JAROWINKLER_SIMILARITY(name_input, 'Catherine') AS jw_score,
        EDITDISTANCE(name_input, 'Catherine')            AS edit_dist,
        SOUNDEX(name_input)                              AS soundex_code,
        SOUNDEX('Catherine')                             AS soundex_ref,
        IFF(
            SOUNDEX(name_input) = SOUNDEX('Catherine'),
            'MATCH', 'NO MATCH'
        )                                                AS soundex_result
    FROM soundex_demo
)
SELECT
    name_input,
    pattern,
    jw_score,
    edit_dist,
    soundex_code,
    soundex_ref,
    soundex_result
FROM scores
ORDER BY jw_score DESC;

/*
C a t h e r i n e
│ │ │ │ │ │ │ │ │
│ └─┘ └─┘ └─┘ └─── 母音（a, e, i, e）→ 全て除去
│   │   │   │
│   t   r   n  ← 子音のみ残る
│   ↓   ↓   ↓
│   3   6   5  ← 子音コード表で変換
│
└── C  ← 先頭文字はそのまま保持（変換しない）

結果：C + 3 + 6 + 5 = C365

| コード | 対応する子音             | 代表的な音   |
| ---- | ---------------------- | ----------  |
| 1    | B, F, P, V             | 唇で出す音    |
| 2    | C, G, J, K, Q, S, X, Z | 喉・舌の摩擦音 |
| 3    | D, T                   | 歯茎音       |
| 4    | L                      | 側面音       |
| 5    | M, N                   | 鼻音         |
| 6    | R                      | R音         |
| -    | A, E, I, O, U, H, W, Y | 母音・半母音  |
*/


-- ============================================================
-- Fin
-- ============================================================















-- ============================================================
-- Extra Mode：日本語検証用テーブル（顧客名表記揺れを想定）
-- ============================================================

CREATE OR REPLACE TABLE member_names (
    member_id   INT,
    name_input  VARCHAR(255),
    name_type   VARCHAR(50)
);

INSERT INTO member_names VALUES
-- 基準：山田 太郎（正規表記）
 (1,  '山田太郎',         '漢字（正規）'),
 (2,  '山田 太郎',        '漢字+スペース'),
 (3,  '山田たろう',       '漢字+ひらがな'),
 (4,  '山田タロウ',       '漢字+カタカナ'),
 (5,  'やまだたろう',     'ひらがな'),
 (6,  'ヤマダタロウ',     'カタカナ'),
 (7,  'Yamada Taro',      'ローマ字'),
 (8,  'yamada taro',      'ローマ字（小文字）'),
 (9,  '山田 太郞',        '旧字体（郎→郞）'),
 (10, '山田太一',         '別人（太一）'),
-- 会社名の揺れ（加盟店マスタ想定）
 (11, '株式会社ツタヤ',   '正規会社名'),
 (12, 'ツタヤ株式会社',   '前後逆転'),
 (13, '（株）ツタヤ',     '略称'),
 (14, 'TSUTAYA',          '英字'),
 (15, 'ＴＳＵＴＡＹＡ',  '全角英字'),
-- 住所の揺れ
 (16, '東京都渋谷区道玄坂',      '正規住所'),
 (17, '東京都渋谷区道玄坂１丁目','丁目付き'),
 (18, '渋谷区道玄坂',            '都道府県なし'),
 (19, '東京都澁谷区道玄坂',      '旧字体（渋→澁）');

-- ============================================================
-- 実務シナリオ別：推奨クエリパターン
-- ============================================================

-- ① 人名名寄せ：JWで候補抽出 → 目視確認
--   （ひらがな・カタカナ統一後に使うと精度UP）
SELECT
    a.member_id   AS id_a,
    a.name_input  AS name_a,
    b.member_id   AS id_b,
    b.name_input  AS name_b,
    JAROWINKLER_SIMILARITY(a.name_input, b.name_input) AS jw_score
FROM member_names a
JOIN member_names b
    ON a.member_id < b.member_id
WHERE JAROWINKLER_SIMILARITY(a.name_input, b.name_input) >= 70
ORDER BY jw_score DESC;


-- ② 会社名名寄せ：EDで細かい差分を検出
--   （「株式会社」「(株)」などの揺れはED=1〜3に収まりやすい）
SELECT
    a.name_input  AS name_a,
    b.name_input  AS name_b,
    EDITDISTANCE(a.name_input, b.name_input) AS edit_dist
FROM member_names a
JOIN member_names b
    ON a.member_id < b.member_id
WHERE a.member_id BETWEEN 11 AND 15
  AND b.member_id BETWEEN 11 AND 15
ORDER BY edit_dist ASC;


-- ③ 住所揺れ検出：前方一致 + EDの組み合わせ
--   （都道府県省略などはLIKEで前処理 → EDで差分確認）
SELECT
    a.name_input   AS address_a,
    b.name_input   AS address_b,
    EDITDISTANCE(a.name_input, b.name_input)            AS edit_dist,
    JAROWINKLER_SIMILARITY(a.name_input, b.name_input)  AS jw_score
FROM member_names a
JOIN member_names b
    ON a.member_id < b.member_id
WHERE a.member_id BETWEEN 16 AND 19
  AND b.member_id BETWEEN 16 AND 19
ORDER BY edit_dist ASC;


-- ============================================================
-- CTE + JOIN を一体で実行（分割実行NG）
-- ============================================================
WITH normalized AS (
    SELECT
        member_id,
        name_input,
        name_type,
        TRIM(
            SNOWFLAKE.CORTEX.AI_COMPLETE(
                'Claude-4-Sonnet',
                CONCAT(
                    'カタカナをひらがなに変換してください。',
                    '漢字・英字・記号・スペースはそのまま維持し、',
                    '変換後のテキストのみを返してください。',
                    'テキスト：', name_input
                )
            )
        ) AS name_normalized
    FROM member_names
    WHERE member_id BETWEEN 1 AND 9
)
SELECT
    a.name_input                                                  AS name_a,
    b.name_input                                                  AS name_b,
    a.name_normalized                                             AS normalized_a,
    b.name_normalized                                             AS normalized_b,
    EDITDISTANCE(a.name_normalized, b.name_normalized)           AS edit_dist,
    JAROWINKLER_SIMILARITY(a.name_normalized, b.name_normalized) AS jw_score
FROM normalized a
JOIN normalized b
    ON a.member_id < b.member_id
ORDER BY jw_score DESC;





SELECT TRANSLATE(
    '山田タロウ',
    'ァアィイゥウェエォオカガキギクグケゲコゴサザシジスズセゼソゾタダチヂッツヅテデトドナニヌネノハバパヒビピフブプヘベペホボポマミムメモャヤュユョヨラリルレロヮワヰヱヲンヴヵヶ',
    'ぁあぃいぅうぇえぉおかがきぎくぐけげこごさざしじすずせぜそぞただちぢっつづてでとどなにぬねのはばぱひびぴふぶぷへべぺほぼぽまみむめもゃやゅゆょよらりるれろゎわゐゑをんゔゕゖ'
) AS converted;


-- ============================================================
-- カタカナ → ひらがな変換 UDF（JavaScript）
-- ============================================================
USE ROLE ACCOUNTADMIN;

GRANT CREATE FUNCTION
    ON SCHEMA FROSTY_FRIDAY.WEEK_092
    TO ROLE FROSTY_FRIDAY;


-- ============================================================
-- KANA_TO_HIRA 修正版（NULL / undefined 両対応）
-- ============================================================
USE ROLE      FROSTY_FRIDAY;
USE WAREHOUSE FROSTY_FRIDAY_XS;
USE DATABASE  FROSTY_FRIDAY;
USE SCHEMA    WEEK_092;

CREATE OR REPLACE FUNCTION KANA_TO_HIRA(str VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
    // null（SQL NULL）と undefined（未定義）を両方ガード
    if (STR == null) return null;

    var result = '';
    for (var i = 0; i < STR.length; i++) {
        var code = STR.charCodeAt(i);
        // カタカナ範囲: U+30A1（ァ）〜 U+30F6（ヶ）
        if (code >= 0x30A1 && code <= 0x30F6) {
            result += String.fromCharCode(code - 0x60);
        } else {
            result += STR[i];
        }
    }
    return result;
$$;

-- 動作確認
SELECT
    KANA_TO_HIRA('ヤマダタロウ')   AS test1,  -- → やまだたろう
    KANA_TO_HIRA('山田タロウ')     AS test2,  -- → 山田たろう
    KANA_TO_HIRA('東京都シブヤ区') AS test3,  -- → 東京都しぶや区
    KANA_TO_HIRA(NULL)             AS test4;  -- → NULL
