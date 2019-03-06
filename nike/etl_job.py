from dependencies.spark import start_spark
from utils.aggregate_ops import get_agg_result
from pyspark.sql.types import DoubleType, IntegerType
def main():
    """Main ETL script definition.
    :return: None
    """
    # Start spark application and get spark session, logger and config
    spark, log, config = start_spark(
        app_name = 'nike_etl_job',
        files = ['configs/etl_config.json']
    )
    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')

    # Execute ETL pipeline
    data = extract_data(spark)
    data_transformed = transformed_data(data, spark)
    load_data(data_transformed)

    # log the success and terminate the spark session
    log.warn('test_etl_job is finished')
    spark.stop()
    return None

def extract_data(spark):
    """Load data from Hive table"""
    # spark.sql("show databases").show()
    spark.sql("use mock1") # This is a hive table
    df = spark.sql(
        "select * from person"
    )
    # df.show()
    return df

def transformed_data(df,spark):
    "Transform the original data set"

    # df_transformed = df.select("Firstname")
    # df_transformed = df.filter(df.Lastname == "Arrow")
    # df_transformed = df.agg({column_name: operation})
    column_name = 'Salary'
    # operation = 'stddev'
    
    df_transformed = get_agg_result(df, column_name).collect()
    return df_transformed
    #https://stackoverflow.com/questions/43444925/how-to-create-dataframe-from-list-in-spark-sql/43445139
    #create dataframe from list in pyspark

def load_data(df):
    df.show()

# Entry point for PySpark ETL application
if __name__ == '__main__':
    main()