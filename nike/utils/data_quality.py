def check_data_quality(spark, df, table_name, column_to_retrieve_after_null_check, 
                        columns_for_null_chk, check_type):
    
    list_of_filtered_cols_after_null_chk = []
    columns_to_check_for_null = []
    for i in range(0, len(columns_for_null_chk)):
        exp_df = spark.sql("select explode({0}) as col{1} from {2}".format(columns_for_null_chk[i], i, table_name)) 
        exp_df.show()
        columns_to_check_for_null.append(exp_df)
        columns_to_check_for_null
    print("Now -->".format(columns_to_check_for_null))

    if(check_type == "is null"):
        for chk in range (0, len(columns_to_check_for_null)):
            list_of_col_data = []
            filter_cols_after_null_chk = {}
            null_check_df = spark.sql(
                "select {0} from {1} where {2} is null or {2} == 'null'".format(column_to_retrieve_after_null_check,
                table_name, columns_to_check_for_null[chk])
                )
            for i in range (0, null_check_df.count()):
                list_of_col_data.append(null_check_df.collect()[i].__getitem__(column_to_retrieve_after_null_check))
            list_of_col_data.insert(0, columns_to_check_for_null[chk])
            filter_cols_after_null_chk[column_to_retrieve_after_null_check] = list_of_col_data
            list_of_filtered_cols_after_null_chk.append(filter_cols_after_null_chk)
    else:
        for chk in range (0, len(columns_to_check_for_null)):
            list_of_col_data = []
            filter_cols_after_null_chk = {}
            null_check_df = spark.sql(
                "select {0} from {1} where {2} is not null or {2} != 'null'".format(column_to_retrieve_after_null_check,
                table_name, columns_to_check_for_null[chk])
                )
            for i in range (0, null_check_df.count()):
                list_of_col_data.append(null_check_df.collect()[i].__getitem__(column_to_retrieve_after_null_check))
            list_of_col_data.insert(0, columns_to_check_for_null[chk])
            filter_cols_after_null_chk[column_to_retrieve_after_null_check] = list_of_col_data
            list_of_filtered_cols_after_null_chk.append(filter_cols_after_null_chk)

    return list_of_filtered_cols_after_null_chk
