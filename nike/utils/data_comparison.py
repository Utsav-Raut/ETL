from pyspark.sql import functions

def compare_data(spark, df, fields_to_compare):
    file_df = spark.read.json()
    for list_of_fields in fields_to_compare:
        df2 = df.join(file_df, [df.list_of_data[0] == file_df.list_of_data[0]],how='inner')
        comp_result = df2.filter(df.list_of_data[1]!=file_df.list_of_data[1]).select(df.list_of_data[0].alias("table".format(list_of_data[0])),file_df.list_of_data[1].
        alias("file".format(list_of_data[0])).df.list_of_data[1].alias("table_data"))
        comp_result.show()
    return comp_result