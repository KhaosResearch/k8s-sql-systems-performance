# Load util functions
source ./trino.sh

deploy_hive_trino

deploy_trino

for SF in 10 100 300
do
  echo -e "\n USING SCALE FACTOR OF $SF \n"
  execute_tpch_in_trino $SF 5
done

# Concatenate the files
cat trino_queries/results/trino_times_tpch_SF_* | paste -d ";" - - > trino_queries/trino_times_tpch.csv

stop_trino
