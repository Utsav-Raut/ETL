import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions
from utils.compare import get_file_details, get_hive_tbl_details
from pyspark.sql.functions import explode, col

def compare_data(spark, df, fields_to_compare, table_name):
    file_df = get_file_details()
    x = fields_to_compare[0]
    print(df[x])
    print("***********")
    for list_of_fields in fields_to_compare: 
        df2 = file_df.join(df, [file_df[list_of_fields] == df[list_of_fields]])
        # df2 = file_df.join(df, [file_df.notifications.POSLog.Transaction.TransactionID == df.notifications.POSLog.Transaction.TransactionID])
        df2.show()
        comp_result = df2.filter(df.list_of_data[1]!=file_df.list_of_data[1]).select(df.list_of_data[0].alias("table".format(list_of_data[0])),file_df.list_of_data[1].
        alias("file".format(list_of_data[0])).df.list_of_data[1].alias("table_data"))
        comp_result.show()
    # return comp_result