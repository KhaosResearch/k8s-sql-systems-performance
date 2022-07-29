HIVE_QUERIES=(
"select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem_@ where l_shipdate <= '1998-09-16' group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus;"
#"Q2"
"select l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority from customer_@ c join orders_@ o on c.c_mktsegment = 'BUILDING' and c.c_custkey = o.o_custkey join lineitem_@ l on l.l_orderkey = o.o_orderkey where o_orderdate < '1995-03-15' and l_shipdate > '1995-03-15' group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate limit 10;"
#"PYSPARK_Q4.py"
#"PYSPARK_Q5.py"
#"PYSPARK_Q6.py"
#"PYSPARK_Q7.py"
)
