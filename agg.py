import findspark
findspark.init()
from pyspark.sql import SparkSession
warehouse_location = "mysql://localhost:3306/hive1"
spark = SparkSession.builder.config('spark.sql.warehouse.dir',warehouse_location).enableHiveSupport().getOrCreate()

spark.sql('use mock1')
#spark.sql("CREATE TABLE IF NOT EXISTS person(Firstname STRING, Lastname STRING, Sex STRING, Age INT, SALARY INT, Expense_Amt INT)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ")

#spark.sql("LOAD DATA LOCAL INPATH '/home/boom/Desktop/main_proj_nike/person.csv' OVERWRITE INTO TABLE person")

df = spark.sql("select * from person")
# df.show()
# df.printSchema()
df.groupBy('Salary').max().show()


