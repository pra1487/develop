from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def transform_data(input_source: str, output_uri: str):
    with SparkSession.builder.appName("EMR sample").getOrCreate() as spark:
        # Load csv file
        df = spark.read.csv(input_source, header=True)

        # Rename columns
        df = df.select(
            col('name').alias("Name"),
            col('Department').alias('Dept_name')
        )

        # Create an in-memory Dataframe
        df.createOrReplaceTempView('my_first_table')

        # Construct SQL query
        group_by_query = """
            select name, count(*) as total_count
            from my_first_table where Dept_name = 'ENG'
            group by name
        """

        # Transform data
        transform_df = spark.sql(group_by_query)

        # log into EMR stdout
        print(f"Number of rows in SQL query: {transform_df.count()}")

        # write out df into parquet format
        transform_df.write.mode("overwrite").parquet(output_uri)