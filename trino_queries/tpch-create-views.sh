TRINO_VIEWS=(
"create view hive.default.revenue_@ as select  l_suppkey as supplier_no,  sum(l_extendedprice * (1 - l_discount)) as total_revenue from  hive.default.lineitem_@ where  date(l_shipdate) >= date '1993-05-01'  and date(l_shipdate) < date '1993-05-01' + interval '3' month group by  l_suppkey;"
)
