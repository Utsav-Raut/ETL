# import pyspark
# from pyspark.sql.functions import avg, stddev
def get_agg_result(df, column_name):
    agg_result = []
    agg_result.append(df.agg({column_name: 'mean'}))
    agg_result.append(df.agg({column_name: 'max'}))
    agg_result.append(df.agg({column_name: 'min'}))
    agg_result.append(df.agg({column_name: 'avg'}))
    agg_result.append(df.agg({column_name: 'stddev'}))
    return agg_result