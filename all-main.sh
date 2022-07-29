echo "EMPIEZA TRINO $(date)" >> all_start_times
./trino-main.sh
echo "EMPIEZA DRILL $(date)" >> all_start_times
./drill-main.sh
echo "EMPIEZA SPARK $(date)" >> all_start_times
./spark-main.sh
echo "EMPIEZA HIVE $(date)" >> all_start_times
./hivemr3-main.sh
echo "¿Ha petado hive? $(date)" >> all_start_times
