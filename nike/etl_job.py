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
    data = extract_data(spark, config['db_name'], config['table_name'])
    data_transformed = transformed_data(data, spark, config['column_name'])
    load_data(data_transformed)

    # log the success and terminate the spark session
    log.warn('test_etl_job is finished')
    spark.stop()
    return None

def extract_data(spark, db_name, table_name):
    """Load data from Hive table"""
    # spark.sql("show databases").show()
    spark.sql("use {0}".format(db_name)) # This is a hive db
    df = spark.sql(
        "select * from {0}".format(table_name)
    )
    # df.show()
    return df

def transformed_data(df, spark, column_name):
    "Transform the original data set"

    # df_transformed = df.select("Firstname")
    # df_transformed = df.filter(df.Lastname == "Arrow")
    # df_transformed = df.agg({column_name: operation})
    # column_name = 'Salary'
    # operation = 'stddev'
    
    df_transformed = get_agg_result(df, column_name)
    return df_transformed

def load_data(df):
    for data in df:
        data.show()

# Entry point for PySpark ETL application
if __name__ == '__main__':
    main()