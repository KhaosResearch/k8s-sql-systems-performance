# Load util functions
source ./drill.sh

for SF in 300
do
  subset=(0)
  echo -e "\n USING SCALE FACTOR OF $SF \n"
  execute_tpch_subset_in_drill $SF 1 "${subset[@]}"
done

