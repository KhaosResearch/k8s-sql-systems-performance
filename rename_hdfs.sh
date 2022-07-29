for table in customer lineitem orders part partsupp supplier # region nation #Region and nation are not partitioned
do
  for SF in 10 100 300
  do
    n=$(( ($SF/10) ))
    for i in $(seq 1 $n )
    do
      echo "Table = $table \n\tSF = $SF \n\t\ti = $i"
      echo hdfs dfs -mv /tpch/tbl/tpc_h_SF_${SF}/${table}/${table}.tbl.${i} /tpch/tbl/tpc_h_SF_${SF}/${table}/${table}.${i}.tbl
    done
  done
done
