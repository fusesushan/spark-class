from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a Spark session
spark = SparkSession.builder.appName("Day2").getOrCreate()

# Load the Chipotle dataset into a Spark DataFrame
data_path = "occupation.csv"  # Replace with the actual path
occupation = spark.read.csv(data_path, header=True, inferSchema=True)

# Print the schema of the DataFrame
occupation.printSchema()

# Select specific columns and show the DataFrame
occupation.select('user_id', 'age', 'occupation').show()

# Filter out rows where age > 30 and show the DataFrame
occupation.filter(col("age") > 30).show()

# Group by occupation and show user count
occupation.groupBy("occupation").count().show()

# Add age groups to the DataFrame
conditions = [
    (col("age").between(18, 25), "18-25"),
    (col("age").between(26, 35), "26-35"),
    (col("age").between(36, 50), "36-50"),
    (col("age") >= 51, "51+")
]

occupation_with_age_group = occupation.withColumn("age_group",
                                                  when(conditions[0][0], conditions[0][1])
                                                 .when(conditions[1][0], conditions[1][1])
                                                 .when(conditions[2][0], conditions[2][1])
                                                 .when(conditions[3][0], conditions[3][1])
                                                 .otherwise("Unknown"))

occupation_with_age_group.show()

# Define a custom schema and create a new DataFrame
schema = StructType([
    StructField("firstname", StringType(), nullable=True),
    StructField("middlename", StringType(), nullable=True),
    StructField("lastname", StringType(), nullable=True),
    StructField("id", StringType(), nullable=True),
    StructField("gender", StringType(), nullable=True),
    StructField("salary", IntegerType(), nullable=True)
])

data = [
    ("James", " ", "Smith", "36636", "M", 3000),
    ("Michael", "Rose", " ", "40288", "M", 4000),
    ("Robert", " ", "Williams", "42114", "M", 4000),
    ("Maria", "Anne", "Jones", "39192", "F", 4000),
    ("Jen", "Mary", "Brown", " ", "F", -1)
]

df_n = spark.createDataFrame(data, schema=schema)

# Show the schema
df_n.printSchema()

# Show the content of the DataFrame without truncation
df_n.show(truncate=False)

# Add a new column "gender" with default value and rename columns
df = occupation.withColumn("gender", lit("Unknown"))
df2 = df.withColumnRenamed("age", "Years")
df2.show()

# Filter and order the DataFrame
df3 = df2.select("*").filter(col("Years") < 30).orderBy(col("Years").desc())
df3.show()

# Coalesce the DataFrame and collect rows
df_coalesced = df_n.coalesce(2)
rows = df_coalesced.collect()
for row in rows:
    print(row)

# Get number of partitions
num_partitions = df_coalesced.rdd.getNumPartitions()
print("Number of partitions =", num_partitions)

# Stop the Spark session
spark.stop()
