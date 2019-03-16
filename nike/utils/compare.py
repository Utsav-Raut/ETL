import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import HiveContext

sc = SparkSession.builder.appName('Compare').getOrCreate()
hc = HiveContext(sc)

df1 = sc.read.json('')
df1.show()
df2 = hc.table() # reading hive table
df2.collect()

join_df = df1.join(df2,[df1.TransactionID == df2.TransactionID])
res_df = join_df.filter(df1.salary != df2.salary).select(df1.TransactionID.alias('TransactionID'),df1.salary.alias('json_salary'),df2.salary.alias('table_salary'))
res_df.show()