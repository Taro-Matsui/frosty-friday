from snowflake.snowpark.functions import col, count

def main(session):
    df_tables = session.table("SNOWFLAKE.ACCOUNT_USAGE.TABLES")
    df_filtered = df_tables.filter(
        (col("IS_TRANSIENT") == "NO") &
        (col("DELETED").is_null()) &
        (col("ROW_COUNT") > 0)
    )
    df_grouped = (
        df_filtered.group_by(col("TABLE_CATALOG"))
        .agg(count(col("TABLE_NAME")).alias("TABLES"))
        .select(
            col("TABLE_CATALOG").alias("DATABASE"),
            col("TABLES")
        )
        .sort(col("TABLES").desc())
    )
    df_grouped.show()
    return df_grouped
