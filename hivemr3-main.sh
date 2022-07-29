# Load util functions
source ./hivemr3.sh

deploy_hive

for SF in 10 100 300
do
  echo "TPC-H in HIVEMR3 for SF $SF"
  execute_tpch_in_hive $SF 15
done

# Concatenate the files
cat hive_queries/results/hive_times_tpch_SF_* | paste -d ";" - - > hive_queries/hive_times_tpch.csv

stop_hive
