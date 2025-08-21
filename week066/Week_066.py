import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col

def main(session: snowpark.Session):
    df_tables = session.table("SNOWFLAKE.ACCOUNT_USAGE.TABLES")
    df_filtered = df_tables.filter(
        (col("IS_TRANSIENT") == "NO") & 
        (col("DELETED").is_null()) & 
        (col("ROW_COUNT") > 0)
    ).select(
        "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "ROW_COUNT"
    )
    df_result = df_filtered.sort(col("ROW_COUNT").desc()).limit(10)  # 行数順に上位10件
    df_result.show()
    return df_result
