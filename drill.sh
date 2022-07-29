# Env variables
DRILL_CHART_NAME=drill
DRILL_NAMESPACE=drill-deployment
DRILL_CHART_PATH=/hadoop4/drill-helm-charts-master/drill/

# Load util functions
source ./utils.sh

deploy_drill () {
  clear_memory

  kubectl create namespace $DRILL_NAMESPACE

  helm install -f $OVERRIDE_VALUES_FOLDER/drill.yml $DRILL_CHART_NAME $DRILL_CHART_PATH -n $DRILL_NAMESPACE

  echo "Chart $DRILL_CHART_NAME deployed, waiting for pods to be ready...";

  # for POD in $(pods_by_keyword $DRILL_CHART_NAME $DRILL_NAMESPACE)
  # do
  #   wait_for_pod $POD $DRILL_NAMESPACE
  # done

  wait_for_all $DRILL_NAMESPACE

  web_service_IP=$(kubectl get service/drillcluster1-web-svc -o jsonpath='{.spec.clusterIP}' -n $DRILL_NAMESPACE)
  curl -X POST -H "Content-Type: application/json" -d '{"name":"hdfs", "config": {"type": "file", "enabled": true, "connection": "hdfs://IP:8020/", "workspaces": { "root": { "location": "/", "writable": true, "defaultInputFormat": null}}, "formats": {"psv": {"type": "text", "extensions": ["tbl"], "delimiter": "|"}}}}' http://${web_service_IP}:8047/storage/myplugin.json
  echo "Pods for $DRILL_CHART_NAME ready.";
}

# create_tpch_tables_drill <tpch SF>
# Example: create_tpch_tables_drill 100
create_tpch_tables_drill () {
  # Load tpch view create
  if source ./drill_queries/tpch-tables.sh; then
    echo ${#DRILL_VIEWS[@]}
    # Execute create tables
    local i=0
    local j=0
    for ((i = 0; i < ${#DRILL_VIEWS[@]}; i++))
    do
      local query=${DRILL_VIEWS[$i]//'@'/${1}}
      echo kubectl exec -ti --namespace $DRILL_NAMESPACE --container drill-pod drillcluster1-drillbit-0 -- /opt/drill/bin/drill-conf -e ${query}
      (time (kubectl exec -ti --namespace $DRILL_NAMESPACE --container drill-pod drillcluster1-drillbit-0 -- /opt/drill/bin/drill-conf -e "${query}") >> "./drill_queries/logs/views_${1}.log")
      sleep 2
      echo Created view ${query}
    done
  else
  echo "View file doesn't exists"
  fi
}

# drop_tpch_tables_drill <tpch SF>
# Example: drop_tpch_tables_drill 100
drop_tpch_tables_drill () {
  # Load tpch view drop
  if source ./drill_queries/tpch-drop-tables.sh; then
    echo ${#DRILL_VIEWS[@]}
    # Execute drop tables
    local i=0
    local j=0
    for ((i = 0; i < ${#DRILL_VIEWS[@]}; i++))
    do
      local query=${DRILL_VIEWS[$i]//'@'/${1}}
      echo kubectl exec -ti --namespace $DRILL_NAMESPACE --container drill-pod drillcluster1-drillbit-0 -- /opt/drill/bin/drill-conf -e ${query}
      (time (kubectl exec -ti --namespace $DRILL_NAMESPACE --container drill-pod drillcluster1-drillbit-0 -- /opt/drill/bin/drill-conf -e "${query}") >> "./drill_queries/logs/views_${1}.log")
      sleep 2
      echo Created view ${query}
    done
  else
  echo "View file doesn't exists"
  fi
}

# execute_tpch_in_drill <tpch SF> <execution number>
# Example: execute_tpch_in_drill 100 0
execute_tpch_in_drill () {
  # Load tpch queries
  if source ./drill_queries/tpch.sh; then
    # echo ${#DRILL_QUERIES[@]}
    # Execute queries
    local i=0
    local j=0
    for ((i = 0; i < ${#DRILL_QUERIES[@]}; i++))
    do
      clear_memory
      create_tpch_tables_drill ${1}
      for ((j = 0; j < ${2}; j++))
      do
        echo "${DRILL_QUERIES[$i]};${1};${j}" >> ./drill_queries/results/drill_times_tpch_SF_${1}_execution_${2}.txt
        local query=${DRILL_QUERIES[$i]//'@'/${1}}
        echo kubectl exec -ti --namespace $DRILL_NAMESPACE --container drill-pod drillcluster1-drillbit-0 -- /opt/drill/bin/drill-conf -e ${query}
        (time (kubectl exec -ti --namespace $DRILL_NAMESPACE --container drill-pod drillcluster1-drillbit-0 -- /opt/drill/bin/drill-conf -e "${query}") >> "./drill_queries/logs/${1}_${2}_$i.log") |& grep "" >> ./drill_queries/results/drill_times_tpch_SF_${1}_execution_${2}.txt
        sleep 2
        echo Executed query ${query}
      done
      drop_tpch_tables_drill ${1}
    done
  else
    echo "Query file doesn't exists"
  fi
}

# TODO
# Example: execute_tpch_subset_in_drill 100 0 array_of_query_indices
execute_tpch_subset_in_drill () {
  # Load tpch queries
  if source ./drill_queries/tpch.sh; then
    TIMEFORMAT=%R
    # Execute queries
    local i=0
    local j=0
    SF=${1}
    REP=${2}
    # https://unix.stackexchange.com/questions/225943/except-the-1st-argument
    shift 2
    for ele in "${@}"
    do
      clear_memory
      create_tpch_tables_drill ${SF}
      echo "${DRILL_QUERIES[$ele]};${SF};${REP}" >> ./drill_queries/results/drill_times_tpch_SF_${SF}_execution_${REP}.txt
      for ((j = 0; j < ${REP}; j++))
      do
        local query=${DRILL_QUERIES[$ele]//'@'/${SF}}
        echo kubectl exec -ti --namespace $DRILL_NAMESPACE --container drill-pod drillcluster1-drillbit-0 -- /opt/drill/bin/drill-conf -e ${query}
        (time (kubectl exec -ti --namespace $DRILL_NAMESPACE --container drill-pod drillcluster1-drillbit-0 -- /opt/drill/bin/drill-conf -e "${query}") >> "./drill_queries/logs/${SF}_${REP}_$i.log") |& grep "" >> ./drill_queries/results/drill_times_tpch_SF_${SF}_execution_${REP}.txt
        sleep 2
        echo Executed query "${query}"
      done
      drop_tpch_tables_drill ${SF}
    done
  else
  echo "Query file doesn't exist"
  fi
}

# Stop the drill deployment
stop_drill () {
  k8s_delete $DRILL_CHART_NAME $DRILL_NAMESPACE
}
