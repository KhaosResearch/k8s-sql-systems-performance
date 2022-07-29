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

dataframe = sqlContext.sql("""select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
from
        lineitem
where
        l_shipdate <= date '1998-09-02'
group by
        l_returnflag,
        l_linestatus
order by
        l_returnflag,
        l_linestatus;""")

count_ = dataframe.count()

end = time.time()
query_ex_time=end-start
print("TIEMPO DE EJECUCION DE CONSULTA SQL ==========>  ", query_ex_time)

with open('/tmp/spark_times.csv','a') as file:
    file.write(str(query_ex_time)+';'+str(count_)+';'+'Q0\n')
