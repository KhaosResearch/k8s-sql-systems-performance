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
########################## CUSTOMER BEGIN
tablename = "customer"


fields=[StructField("C_CUSTKEY", IntegerType(), True),
        StructField("C_NAME", StringType(),True),
        StructField("C_ADDRESS", StringType(),True),
        StructField("C_NATIONKEY", IntegerType(),True),
        StructField("C_PHONE", StringType(),True),
        StructField("C_ACCTBAL", FloatType(),True),
        StructField("C_MKTSEGMENT", StringType(),True),
        StructField("C_COMMENT", StringType(),True)]

schema=StructType(fields)

df = sqlContext.read.format("csv").schema(schema).load(basePath + f'{tablename}/*')

df.createOrReplaceTempView(tablename)
########################## CUSTOMER END

########################## LINEITEM BEGIN
tablename = "lineitem"


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

df = sqlContext.read.format("csv").schema(schema).load(basePath + f'{tablename}/*')

df.createOrReplaceTempView(tablename)
########################## LINEITEM END

########################## NATION BEGIN
tablename = "nation"


fields=[
        StructField("N_NATIONKEY", IntegerType(), True),
        StructField("N_NAME", StringType(), True),
        StructField("N_REGIONKEY", IntegerType(), True),
        StructField("N_COMMENT", StringType(), True)]
schema=StructType(fields)
df = sqlContext.read.format("csv").schema(schema).load(basePath + f'{tablename}/*')

df.createOrReplaceTempView(tablename)
########################## NATION END

########################## ORDERS BEGIN
tablename = "orders"


fields=[StructField("O_ORDERKEY", IntegerType(), True),
        StructField("O_CUSTKEY", IntegerType(),True),
        StructField("O_ORDERSTATUS", StringType(),True),
        StructField("O_TOTALPRICE", FloatType(),True),
        StructField("O_ORDERDATE", StringType(),True),
        StructField("O_ORDERPRIORITY", StringType(),True),
        StructField("O_CLERK", StringType(),True),
        StructField("O_SHIPPRIORITY", IntegerType(),True),
        StructField("O_COMMENT", StringType(),True)]

schema=StructType(fields)

df = sqlContext.read.format("csv").schema(schema).load(basePath + f'{tablename}/*')

df.createOrReplaceTempView(tablename)
########################## ORDERS END

########################## PART BEGIN
tablename = "part"


fields=[
        StructField("P_PARTKEY", IntegerType(), True),
        StructField("P_NAME", StringType(),True),
        StructField("P_MFGR", StringType(),True),
        StructField("P_BRAND", StringType(),True),
        StructField("P_TYPE", StringType(),True),
        StructField("P_SIZE", IntegerType(),True),
        StructField("P_CONTAINER", StringType(),True),
        StructField("P_RETAILPRICE", FloatType(),True),
        StructField("P_COMMENT", StringType(),True)]

schema=StructType(fields)

df = sqlContext.read.format("csv").schema(schema).load(basePath + f'{tablename}/*')

df.createOrReplaceTempView(tablename)
########################## PART END

########################## PARTSUPP BEGIN
tablename = "partsupp"


fields=[
        StructField("PS_PARTKEY", IntegerType(), True),
        StructField("PS_SUPPKEY", IntegerType(),True),
        StructField("PS_AVAILQTY", IntegerType(),True),
        StructField("PS_SUPPLYCOST", FloatType(),True),
        StructField("PS_COMMENT", StringType(),True)]
schema=StructType(fields)

df = sqlContext.read.format("csv").schema(schema).load(basePath + f'{tablename}/*')

df.createOrReplaceTempView(tablename)
########################## PARTSUPP END

########################## REGION BEGIN
tablename = "region"


fields=[StructField("R_REGIONKEY", IntegerType(), True),
        StructField("R_NAME", StringType(),True),
        StructField("R_COMMENT", StringType(),True)]

schema=StructType(fields)
df = sqlContext.read.format("csv").schema(schema).load(basePath + f'{tablename}/*')

df.createOrReplaceTempView(tablename)
########################## REGION END

########################## SUPPLIER BEGIN
tablename = "supplier"


fields=[StructField("S_SUPPKEY", IntegerType(), True),
        StructField("S_NAME", StringType(),True),
        StructField("S_ADDRESS", StringType(),True),
        StructField("S_NATIONKEY", IntegerType(),True),
        StructField("S_PHONE", StringType(),True),
        StructField("S_ACCTBAL", FloatType(),True),
        StructField("S_COMMENT", StringType(),True)]

schema=StructType(fields)

df = sqlContext.read.format("csv").schema(schema).load(basePath + f'{tablename}/*')

df.createOrReplaceTempView(tablename)
########################## SUPPLIER END

# Start and time the query
start = time.time()

dataframe = sqlContext.sql("""
select
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice,
	sum(l_quantity)
from
	customer,
	orders,
	lineitem
where
	o_orderkey in (
		select
			l_orderkey
		from
			lineitem
		group by
			l_orderkey having
				sum(l_quantity) > 300
	)
	and c_custkey = o_custkey
	and o_orderkey = l_orderkey
group by
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice
order by
	o_totalprice desc,
	o_orderdate
LIMIT 100;
""")

count_ = dataframe.count()

end = time.time()
query_ex_time=end-start
print("TIEMPO DE EJECUCION DE CONSULTA SQL ==========>  ", query_ex_time,"   ",count_)