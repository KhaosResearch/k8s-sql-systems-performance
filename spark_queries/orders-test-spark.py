import time
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
import sys

# Receive as parameter the Scale Factor
SF=sys.argv[1]

conf = SparkConf().set('spark.memory.fraction', '1.0').set('spark.memory.storage', '0.0').set('spark.sql.exchange.reuse', False)
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
sqlContext.clearCache()

basePath = f"hdfs://IP:8020/tpch/tbl-parquet/tpc_h_SF_{SF}/"

# Create table views
orders = sqlContext.read.option("basePath", basePath).parquet(basePath + 'orders/*')
orders.createOrReplaceTempView("orders")

# Start and time the query
start = time.time()

dataframe = sqlContext.sql("""select * from orders limit 10;""")

dataframe.show()

end = time.time()
query_ex_time=end-start
print("TIEMPO DE EJECUCION DE CONSULTA SQL ==========>  ", query_ex_time)
