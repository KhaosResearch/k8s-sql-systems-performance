import time
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark import SparkContext, SparkConf

conf = SparkConf().set('spark.memory.fraction', '1.0').set('spark.memory.storage', '0.0').set('spark.sql.exchange.reuse', False)
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
sqlContext.clearCache()

basePath = "hdfs://IP:8020/tpch/tbl-parquet/"


lineitem = sqlContext.read.option("basePath", basePath).parquet(basePath)
lineitem.createOrReplaceTempView("lineitem")

start = time.time()

dataframe = sqlContext.sql("""select * from lineitem limit 1;""").collect()

dataframe.show()

end = time.time()
print("TIEMPO DE EJECUCION DE CONSULTA SQL ==========>  ", end-start)
