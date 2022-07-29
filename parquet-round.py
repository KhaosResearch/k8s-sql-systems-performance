import time
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf

conf = SparkConf().set('spark.memory.fraction', '1.0').set('spark.memory.storage', '0.0').set('spark.sql.exchange.reuse', False)
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
sqlContext.clearCache()

for SF in (100, 300):
    for index in range(1, int(SF/10) + 1):
        basePath = f"hdfs://IP:8020/tpch/tbl-parquet/tpc_h_SF_{SF}/"

        # Create table views
        customer = sqlContext.read.option("basePath", basePath).parquet(basePath + f"customer/customer.tbl.{index}")
        customer.createOrReplaceTempView("customer")
        # lineitem = sqlContext.read.option("basePath", basePath).parquet(basePath + f"lineitem/lineitem.tbl.{index}")
        # lineitem.createOrReplaceTempView("lineitem")
        # nation = sqlContext.read.option("basePath", basePath).parquet(basePath + f"nation/nation.tbl.{index}")
        # nation.createOrReplaceTempView("nation")
        # orders = sqlContext.read.option("basePath", basePath).parquet(basePath + f"orders/orders.tbl.{index}")
        # orders.createOrReplaceTempView("orders")
        # part = sqlContext.read.option("basePath", basePath).parquet(basePath + f"part/part.tbl.{index}")
        # part.createOrReplaceTempView("part")
        # partsupp = sqlContext.read.option("basePath", basePath).parquet(basePath + f"partsupp/partsupp.tbl.{index}")
        # partsupp.createOrReplaceTempView("partsupp")
        # region = sqlContext.read.option("basePath", basePath).parquet(basePath + f"region/region.tbl.{index}")
        # region.createOrReplaceTempView("region")
        # supplier = sqlContext.read.option("basePath", basePath).parquet(basePath + f"supplier/supplier.tbl.{index}")
        # supplier.createOrReplaceTempView("supplier")

        # Start and time the query
        start = time.time()

        dataframe = sqlContext.sql("""select
                C_CUSTKEY,
                C_NAME,
                C_ADDRESS,
                C_NATIONKEY,
                C_PHONE,
                round(C_ACCTBAL, 3) as C_ACCTBAL,
                C_MKTSEGMENT,
                C_COMMENT
        from
                customer;""")

        out_file = basePath.replace("tbl-parquet", "tbl-parquet-rounded")

        dataframe.write.mode("overwrite").parquet(out_file + f"customer/customer.tbl.{index}")
