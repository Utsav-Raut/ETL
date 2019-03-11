from dependencies.spark import start_spark
from utils.aggregate_operations import get_agg_result
from utils.row_count_validation import get_row_count_diff
from pyspark.sql.types import DoubleType, IntegerType
from utils.data_quality import check_data_quality
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
    data_transformed = transformed_data(data, spark, config['column_with_agg_func'], 
                        config['table_name'], config['field_name'],
                        config['column_to_retrieve_after_null_check'],
                        config['columns_to_check_for_null'],
                        config['check_type'])
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

def transformed_data(df, spark, column_with_agg_func, table_name, field_name,
                    column_to_retrieve_after_null_check, columns_to_check_for_null, check_type):
    "Transform the original data set"
    # df.coalesce(1).write.csv("D:/Users/URaut/Pictures/output1.csv")
    row_count_diff, result = get_row_count_diff(spark, df, field_name, table_name)
    agg_output = get_agg_result(df, column_with_agg_func)
    list_of_col_values_after_null_check = check_data_quality(spark, df, table_name, column_to_retrieve_after_null_check, columns_to_check_for_null, check_type)

    for x in list_of_col_values_after_null_check:
        x.show()
    df_transformed = [row_count_diff, result, agg_output]

    return df_transformed

def load_data(df_transformed):
    print('hello')
    # for data in df:
    #     data.show()

# Entry point for PySpark ETL application
if __name__ == '__main__':
    main()