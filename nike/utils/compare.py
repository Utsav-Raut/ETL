import findspark
findspark.init()

from pyspark.sql import SparkSession
# from pyspark.sql import functions as f
# from pyspark.sql import HiveContext

# sc = SparkSession.builder.appName('Compare').getOrCreate()
# hc = HiveContext(sc)

# df1 = sc.read.json('/home/boom/Desktop/main_proj_nike/demo_table.json')
# df1.show()
# df2 = hc.table('mock1.demo_table') # reading hive table
# df2.collect()
# df2.show()
# join_df = df1.join(df2,[df1.TransactionID == df2.TransactionID])
# res_df = join_df.filter(df1.salary != df2.salary).select(df1.TransactionID.alias('TransactionID'),df1.salary.alias('json_salary'),df2.salary.alias('table_salary'))
# res_df.show()

def get_file_details():
    spark = SparkSession.builder.appName('Read File').getOrCreate()
    df = spark.read.json('/home/boom/Desktop/main_proj_nike/demo_table.json')
    # df.show()
    return df

def get_hive_tbl_details():
    spark = SparkSession.builder.appName('Read Table').enableHiveSupport().getOrCreate()
    spark.sql('use mock1')
    df = spark.sql('select * from demo_table')
    # print(df)
    # df.show()
    return df

def compare_data():
    df2 = get_hive_tbl_details()
    df1 = get_file_details()
    df1.show()
    df2.show()

    join_df = df1.join(df2,[df1.notifications.POSLog.Transaction.TransactionID == df2.notifications.POSLog.Transaction.TransactionID])
    res_df = join_df.filter(df1.notifications.POSLog.Transaction.TransactionAmt != df2.notifications.POSLog.Transaction.TransactionAmt).select(df1.notifications.POSLog.Transaction.TransactionID.alias('TransactionID'),df1.notifications.POSLog.Transaction.TransactionAmt.alias('json_TxnAmt'),df2.notifications.POSLog.Transaction.TransactionAmt.alias('table_TxnAmt'))
    # res_df.show()
    # print(join_df)
    # join_df.show()

compare_data()