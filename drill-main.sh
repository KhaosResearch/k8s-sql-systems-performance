#!/bin/bash
# Load util functions
source ./drill.sh

deploy_drill

for SF in 10 100 300
do
  echo "Execution with SF $SF"
  execute_tpch_in_drill $SF 15
done

# Concatenate the files
cat drill_queries/results/drill_times_tpch_SF_* | paste -d ";" - - > drill_queries/results/drill_times_tpch.csv

stop_drill
