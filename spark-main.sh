# Load util functions
source ./spark.sh

deploy_spark

for SF in 10 100 300
do
  echo "TPC-H in SPARK for SF $SF"
  execute_tpch_in_spark $SF 15
done

# Concatenate the files
cat spark_queries/spark_times_tpch_SF_* | paste -d ";" - > spark_queries/spark_times_tpch.csv

stop_spark
