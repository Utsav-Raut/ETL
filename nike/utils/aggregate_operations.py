def get_agg_result(df, column_with_agg_func):
    agg_result = []
    for list_of_data in column_with_agg_func:
        for i in range(1, len(list_of_data)):
            agg_result.append(df.agg({list_of_data[0]: format(list_of_data[i])}))
    return agg_result