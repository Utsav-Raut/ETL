def get_agg_result(df, column_name, agg_operation):
    agg_result = df.agg({column_name: agg_operation})
    return agg_result