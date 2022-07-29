# Load util functions
source ./spark.sh

for SF in 300
do
  subset=(21)
  echo -e "\n USING SCALE FACTOR OF $SF \n"
  execute_tpch_subset_in_spark $SF 1 "${subset[@]}"
done

