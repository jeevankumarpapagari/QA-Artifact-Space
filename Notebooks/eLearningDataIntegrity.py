# Databricks PySpark Data Quality Notebook
# Generated from MD validation rules

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, current_date

spark = SparkSession.builder.getOrCreate()

catalog = "edc_etl_qa"
schema = "gold"
table = "elearning_status"
full_table = f"{catalog}.{schema}.{table}"

print(f"Validating table: {full_table}")

def assert_zero(df, message):
    cnt = df.count()
    if cnt > 0:
        raise Exception(f"FAILED: {message} | Count = {cnt}")
    else:
        print(f"PASS: {message}")

def assert_condition(condition, message):
    if not condition:
        raise Exception(f"FAILED: {message}")
    else:
        print(f"PASS: {message}")

# Table existence
df_exists = spark.sql(f"""
SELECT COUNT(*) as cnt
FROM system.information_schema.tables
WHERE table_catalog='{catalog}'
AND table_schema='{schema}'
AND table_name='{table}'
""")
assert_condition(df_exists.collect()[0]['cnt'] == 1, "Table exists")

# Delta metadata
detail_df = spark.sql(f"DESCRIBE DETAIL {full_table}")
row = detail_df.collect()[0]

assert_condition(row['format'] == 'delta', "Delta format")
assert_condition(row['numFiles'] > 0, "Files exist")
assert_condition(row['sizeInBytes'] > 0, "Data exists")

# Load table
df = spark.read.table(full_table)
#df.cache()

print("Total records:", df.count())

# Null key check
assert_zero(
    df.filter(col("MedrioUserId").isNull() | col("CourseId").isNull()),
    "Null keys"
)

# Duplicate check
dup_df = df.groupBy("MedrioUserId", "CourseId").count().filter(col("count") > 1)
assert_zero(dup_df, "Duplicate records")

# Email validation
assert_zero(
    df.filter(~col("Email").rlike(r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')),
    "Invalid emails"
)

# Business rules
assert_zero(
    df.filter((col("IsCompleted") == True) & col("CourseCompletionDate").isNull()),
    "Completed must have completion date"
)

assert_zero(
    df.filter((col("IsCompleted") == False) & col("CourseCompletionDate").isNotNull()),
    "Not completed must not have completion date"
)

# Date checks
assert_zero(
    df.filter(col("CourseCompletionDate") < col("CourseRegDate")),
    "Invalid date sequence"
)

assert_zero(
    df.filter(
        (col("CourseRegDate") > current_date()) |
        (col("CourseCompletionDate") > current_date())
    ),
    "Future dates"
)

# Enum validation
valid_status = ["Registered", "In Progress", "Completed", "Failed", "Dropped", "paid"]

assert_zero(
    df.filter(~col("CourseStatus").isin(valid_status)),
    "Invalid status"
)

print("ALL CHECKS PASSED")
