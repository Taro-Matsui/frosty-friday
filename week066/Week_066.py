import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col

def main(session: snowpark.Session):
    df = session.table("SNOWFLAKE.ACCOUNT_USAGE.TABLES")
    
    df_result = (
        df.filter(
            col("TABLE_CATALOG").isin("SNOWFLAKE", "FROSTY_FRIDAY") &
            (col("IS_TRANSIENT") == "NO") &
            (col("DELETED").is_null()) &
            (col("ROW_COUNT") > 0)
        )
        .group_by("TABLE_CATALOG")
        .count()
        .select(
            col("TABLE_CATALOG").alias("DATABASE_NAME"),
            col("COUNT").alias("TABLE_COUNT")
        )
        .sort(col("DATABASE_NAME"))
    )
    
    return df_result
