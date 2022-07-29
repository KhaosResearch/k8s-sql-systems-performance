from glob import glob
import datetime

from pyspark import SparkContext
from pyspark.sql.types import *
from decimal import *
from pyspark.sql import *
from pyspark.sql import functions as F

sc = SparkContext()
spark = SparkSession(sc)
for SF in (10, 100, 300):
    for index in range(1, int(SF/10) + 1):

        file_ = f"hdfs://IP:8020/tpch/tbl/tpc_h_SF_{SF}/orders.tbl.{index}"
        rdd = sc.textFile(file_)

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

        df = rdd.\
          map(lambda x: x.split("|")).\
          map(lambda x: {
            'O_ORDERKEY':int(x[0]),
            'O_CUSTKEY':int(x[1]),
            'O_ORDERSTATUS':x[2],
            'O_TOTALPRICE':float(x[3]),
            'O_ORDERDATE': x[4],
            'O_ORDERPRIORITY':x[5],
            'O_CLERK':x[6],
            'O_SHIPPRIORITY':int(x[7]),
            'O_COMMENT':x[8],
            })\
          .toDF(schema)

        #df.show(n=2)
        out_file = file_.replace("/tbl/", "/tbl-parquet/")
        print("CREATING PARQUET FILES: ", out_file)
        df.write.mode("overwrite").parquet(out_file)
