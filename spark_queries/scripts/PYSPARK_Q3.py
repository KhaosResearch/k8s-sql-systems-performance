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
customer = sqlContext.read.option("basePath", basePath).parquet(basePath + 'customer.*')
customer.createOrReplaceTempView("customer")
lineitem = sqlContext.read.option("basePath", basePath).parquet(basePath + 'lineitem.*')
lineitem.createOrReplaceTempView("lineitem")
nation = sqlContext.read.option("basePath", basePath).parquet(basePath + 'nation.*')
nation.createOrReplaceTempView("nation")
orders = sqlContext.read.option("basePath", basePath).parquet(basePath + 'orders.*')
orders.createOrReplaceTempView("orders")
part = sqlContext.read.option("basePath", basePath).parquet(basePath + 'part.*')
part.createOrReplaceTempView("part")
partsupp = sqlContext.read.option("basePath", basePath).parquet(basePath + 'partsupp.*')
partsupp.createOrReplaceTempView("partsupp")
region = sqlContext.read.option("basePath", basePath).parquet(basePath + 'region.*')
region.createOrReplaceTempView("region")
supplier = sqlContext.read.option("basePath", basePath).parquet(basePath + 'supplier.*')
supplier.createOrReplaceTempView("supplier")

# Start and time the query
start = time.time()

dataframe = sqlContext.sql("""
select 
  l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority 
from 
  customer c join orders o 
    on c.c_mktsegment = 'BUILDING' and c.c_custkey = o.o_custkey 
  join lineitem l 
    on l.l_orderkey = o.o_orderkey
where 
  o_orderdate < '1995-03-15' and l_shipdate > '1995-03-15' 
group by l_orderkey, o_orderdate, o_shippriority 
order by revenue desc, o_orderdate 
limit 10;
""")

count_ = dataframe.count()

end = time.time()
query_ex_time=end-start
print("TIEMPO DE EJECUCION DE CONSULTA SQL ==========>  ", query_ex_time)

with open('/tmp/spark_times.csv','a') as file:
    file.write(str(query_ex_time)+';'+str(count_)+';'+'Q0\n')
