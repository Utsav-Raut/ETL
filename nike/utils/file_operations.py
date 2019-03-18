import findspark
findspark.init()
from pyspark.sql import SparkSession

def get_file_row_count(file_path, field_name):
    spark = SparkSession.builder.appName("Anything").getOrCreate()
    # tra_list= df.select('notifications.POSLog.Transaction.TransactionID').collect()
    df = spark.read.json(file_path)
    tra_list= df.select(field_name).collect()
    # tra_list
    # tra_list[0].TransactionID
    return len(tra_list)
