import time
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
import sys

from glob import glob

from pyspark.sql.types import *
from decimal import *
from pyspark.sql import *

# Receive as parameter the Scale Factor
SF=sys.argv[1]

conf = SparkConf().set('spark.memory.fraction', '1.0').set('spark.memory.storage', '0.0').set('spark.sql.exchange.reuse', False)
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
sqlContext.clearCache()

# Ericsson
basePath = f"hdfs://IP:8020/tpch/tbl/tpc_h_SF_{SF}/"
# Local
#basePath = f"hdfs://192.168.213.23:9000/tpc_data/tpc_h_SF_{SF}/"

# Create table views
########################## LINEITEM BEGIN
tablename = "lineitem"

rdd = sc.textFile(basePath + f'{tablename}/*')

fields=[
        StructField("L_ORDERKEY", IntegerType(), True),
        StructField("L_PARTKEY", IntegerType(),True),
        StructField("L_SUPPKEY", IntegerType(),True),
        StructField("L_LINENUMBER", IntegerType(),True),
        StructField("L_QUANTITY", FloatType(),True),
        StructField("L_EXTENDEDPRICE", FloatType(),True),
        StructField("L_DISCOUNT", FloatType(),True),
        StructField("L_TAX", FloatType(),True),
        StructField("L_RETURNFLAG", StringType(),True),
        StructField("L_LINESTATUS", StringType(),True),
        StructField("L_SHIPDATE", StringType(),True),
        StructField("L_COMMITDATE", StringType(),True),
        StructField("L_RECEIPTDATE", StringType(),True),
        StructField("L_SHIPINSTRUCT", StringType(),True),
        StructField("L_SHIPMODE", StringType(),True),
        StructField("L_COMMENT", StringType(),True)]

schema=StructType(fields)

df = rdd.\
        map(lambda x: x.split("|")).\
        map(lambda x: {
        'L_ORDERKEY':int(x[0]),
        'L_PARTKEY': int(x[1]),
        'L_SUPPKEY': int(x[2]),
        'L_LINENUMBER': int(x[3]),
        'L_QUANTITY':float(x[4]),
        'L_EXTENDEDPRICE':float(x[5]),
        'L_DISCOUNT':float(x[6]),
        'L_TAX':float(x[7]),
        'L_RETURNFLAG':x[8],
        'L_LINESTATUS':x[9],
        'L_SHIPDATE':x[10],
        'L_COMMITDATE':x[11],
        'L_RECEIPTDATE':x[12],
        'L_SHIPINSTRUCT':x[13],
        'L_SHIPMODE':x[14],
        'L_COMMENT':x[15]
        })\
        .toDF(schema)

df.createOrReplaceTempView(tablename)
########################## LINEITEM END


# Start and time the query
start = time.time()

dataframe = sqlContext.sql("""
select
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
	l_shipdate <= date '1998-12-01' - interval '120' day
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus
LIMIT 1;
""")

count_ = dataframe.count()

end = time.time()
query_ex_time=end-start
print("TIEMPO DE EJECUCION DE CONSULTA SQL ==========>  ", query_ex_time,"   ",count_)
