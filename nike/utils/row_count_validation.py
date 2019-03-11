# def row_count_val(spark, df, field_name, table_name):
#     tbl_row_count_df = spark.sql(
#         "select count({0}) from {1}".format(field_name, table_name)
#     )
#     # tbl_row_count_df.show()
#     tbl_row_count = tbl_row_count_df.head()[0]
#     print(tbl_row_count)
#     return tbl_row_count

from utils import hive_table_operations, file_operations

def get_row_count_diff(spark, df, field_name, table_name):
    hive_tbl_row_count = hive_table_operations.get_hive_tbl_row_count(spark, df, field_name, table_name)
    input_file_row_count = file_operations.get_file_row_count()

    row_count_diff = hive_tbl_row_count - input_file_row_count

    if row_count_diff == 0:
        output = 'Hive Table and files have equal number of records'
    elif row_count_diff > 0:
        output = 'Hive Table has more records than the files'
    else:
        output = 'Files have more data than Hive table'
    # df_transformed = get_agg_result(df, column_name)
    return row_count_diff, output