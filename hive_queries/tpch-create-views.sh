HIVE_VIEWS=(
"create view revenue_@ (supplier_no, total_revenue) as select  l_suppkey,  sum(l_extendedprice * (1 - l_discount)) from  lineitem_@ where  l_shipdate >= date '1993-05-01'  and l_shipdate < date '1993-05-01' + interval '3' month group by  l_suppkey;"
)
