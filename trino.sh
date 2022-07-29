# Env variables
HIVE_CHART_PATH=/hadoop4/automatic_benchmark/hive_gradiant/hive/
HIVE_NAMESPACE=my-hive
HIVE_CHART_NAME=my-hive

TRINO_NAMESPACE=trino
TRINO_CHART=valeriano-manassero/trino
TRINO_CHART_NAME=trino

# Load util functions
source ./utils.sh

# Deploy Hive for Trino
deploy_hive_trino () {
  # Clear OS caches
  clear_memory

  # Create namespace
  kubectl create namespace $HIVE_NAMESPACE;

  kubectl create -f $OVERRIDE_VALUES_FOLDER/hive-pv.yml

  helm install $HIVE_CHART_NAME $HIVE_CHART_PATH -n $HIVE_NAMESPACE

  echo "Chart $HIVE_CHART_NAME deployed, waiting for pods to be ready...";

  wait_for_all $HIVE_NAMESPACE
  
  echo "Pods for $HIVE_CHART_NAME ready.";

}  

deploy_trino () {
  # TRINO INSTALLATION
  kubectl create namespace $TRINO_NAMESPACE

  POD_NAME=$(kubectl get pods --no-headers -o custom-columns=":metadata.name" -n $HIVE_NAMESPACE | grep metastore)
  POD_IP=$(kubectl get pod $POD_NAME --template={{.status.podIP}} -n $HIVE_NAMESPACE)
  sed -i "48s/.*/    hive.metastore.uri=thrift:\/\/$POD_IP:9083/" $OVERRIDE_VALUES_FOLDER/trino.yml
  helm install -f $OVERRIDE_VALUES_FOLDER/trino.yml $TRINO_CHART_NAME $TRINO_CHART -n $TRINO_NAMESPACE

  wait_for_all $TRINO_NAMESPACE

}


# create_tpch_tables <tpch SF>
# Example: create_tpch_tables 100
create_tpch_tables () {
  # Load tpch tables
  if source ./hive_queries/tpch-tables.sh; then
    TIMEFORMAT=%R
    POD_NAME=$(kubectl get pods --no-headers -o custom-columns=":metadata.name" -n $HIVE_NAMESPACE | grep server)
    POD_IP=$(kubectl get pod $POD_NAME --template={{.status.podIP}} -n $HIVE_NAMESPACE)
    # Execute statements
    local i=0
    for ((i = 0; i < ${#HIVE_TABLES[@]}; i++))
    do
      echo "${HIVE_TABLES[$i]};${1}"
      query=${HIVE_TABLES[$i]//'@'/${1}}
      kubectl exec $POD_NAME -it -n $HIVE_NAMESPACE -- /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "${query}" > /dev/null
      sleep 2
      echo Executed query ${query}
    done
  else
  echo "Query file doesn't exists"
  fi
}

# create_revenue_view <tpch SF>
# Example: create_revenue_view 100
create_revenue_view () {
  # Load view query
  if source ./trino_queries/tpch-create-views.sh; then
    POD_NAME=$(kubectl get pods --no-headers -o custom-columns=":metadata.name" -n $TRINO_NAMESPACE | grep trino-coordinator)
    local i=0
    for ((i = 0; i < ${#TRINO_VIEWS[@]}; i++))
    do
      echo "${TRINO_VIEWS[$i]}; ${1}"
      query=${TRINO_VIEWS[$i]//'@'/${1}}
      kubectl exec --stdin --tty --namespace $TRINO_NAMESPACE $POD_NAME -- trino --execute "${query}" > /dev/null
      sleep 2
      echo Executed query ${query}
    done
  fi
}

# drop_tpch_tables <tpch SF>
# Example: drop_tpch_tables 100
drop_tpch_tables () {
  # Load tpch tables
  if source ./hive_queries/tpch-drop-tables.sh; then
    TIMEFORMAT=%R
    POD_NAME=$(kubectl get pods --no-headers -o custom-columns=":metadata.name" -n $HIVE_NAMESPACE | grep server)
    POD_IP=$(kubectl get pod $POD_NAME --template={{.status.podIP}} -n $HIVE_NAMESPACE)
    # Execute statements
    local i=0
    for ((i = 0; i < ${#HIVE_TABLES[@]}; i++))
    do
      echo "${HIVE_TABLES[$i]};${1}"
      query=${HIVE_TABLES[$i]//'@'/${1}}
      echo kubectl exec $POD_NAME -it -n $HIVE_NAMESPACE -- /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "${query}"
      kubectl exec $POD_NAME -it -n $HIVE_NAMESPACE -- /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e "${query}" > /dev/null
      sleep 2
      echo Executed query ${query}
    done
  else
  echo "Query file doesn't exists"
  fi
}

# drop_revenue_view <tpch SF>
# Example: drop_revenue_view 100
drop_revenue_view () {
  # Load view query
  if source ./trino_queries/tpch-drop-views.sh; then
    POD_NAME=$(kubectl get pods --no-headers -o custom-columns=":metadata.name" -n $TRINO_NAMESPACE | grep trino-coordinator)  
    local i=0
    for ((i = 0; i < ${#TRINO_VIEWS[@]}; i++))
    do
      echo "${TRINO_VIEWS[$i]}; ${1}"
      query=${TRINO_VIEWS[$i]//'@'/${1}}
      kubectl exec --stdin --tty --namespace $TRINO_NAMESPACE $POD_NAME -- trino --execute "${query}" > /dev/null
      sleep 2
      echo Executed query ${query}
    done
  fi
}

# Deletes hive workers that have been deployed after a hive task
remove_hive_workers () {
  kubectl get pods --no-headers -o custom-columns=":metadata.name" -n $HIVE_NAMESPACE | awk '/mr3worker/{print $1}' | xargs kubectl delete -n $HIVE_NAMESPACE pod  
}

# execute_tpch_in_trino <tpch SF> <execution number>
# Example: execute_tpch_in_trino 100 0
execute_tpch_in_trino () {
  # Load tpch queries
  if source ./trino_queries/tpch.sh; then
    TIMEFORMAT=%R
    POD_NAME=$(kubectl get pods --no-headers -o custom-columns=":metadata.name" -n $TRINO_NAMESPACE | grep trino-coordinator)
    # Execute queries
    local i=0
    local j=0
    for ((i = 0; i < ${#TRINO_QUERIES[@]}; i++))
    do
      clear_memory
      create_tpch_tables ${1}
      create_revenue_view ${1}
      echo "${TRINO_QUERIES[$i]//'@'/${1}};${1}" >> ./trino_queries/results/trino_times_tpch_SF_${1}_executions_${2}.txt
      for ((j = 0; j < ${2}; j++))
      do
        query=${TRINO_QUERIES[$i]//'@'/${1}}
        echo kubectl exec --stdin --tty --namespace $TRINO_NAMESPACE $POD_NAME -- trino --execute "${query}"
        kubectl exec --stdin --tty --namespace $TRINO_NAMESPACE $POD_NAME -- trino --execute "${query}" > /dev/null
        QUERY_RESULT="SELECT date_diff('millisecond',\"created\",\"end\")/1000.0000, query from system.runtime.queries ORDER BY created DESC OFFSET 1 limit 1;"
        kubectl exec --stdin --tty --namespace $TRINO_NAMESPACE $POD_NAME -- trino --execute "$QUERY_RESULT" >> "./trino_queries/logs/${1}_${2}_$i.log" |& grep "" >> ./trino_queries/results/trino_times_tpch_SF_${1}_executions_${2}.txt  
        sleep 2
        echo Executed query "${query}"
      done
      drop_revenue_view ${1}
      drop_tpch_tables ${1}
    done
  else
  echo "Query file doesn't exist"
  fi
}


# Example: execute_tpch_subset_in_trino 100 0 array_of_query_indices
execute_tpch_subset_in_trino () {
  # Load tpch queries
  if source ./trino_queries/tpch.sh; then
    TIMEFORMAT=%R
    POD_NAME=$(kubectl get pods --no-headers -o custom-columns=":metadata.name" -n $TRINO_NAMESPACE | grep trino-coordinator)
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
      create_tpch_tables ${SF}
      create_revenue_view ${SF}
      echo "${TRINO_QUERIES[$ele]//'@'/${SF}};${SF}" >> ./trino_queries/results/trino_times_tpch_SF_${SF}_executions_${REP}.txt
      for ((j = 0; j < ${REP}; j++))
      do
        query="${TRINO_QUERIES[$ele]//'@'/${SF}}"
        echo kubectl exec --stdin --tty --namespace $TRINO_NAMESPACE $POD_NAME -- trino --execute "${query}"
        kubectl exec --stdin --tty --namespace $TRINO_NAMESPACE $POD_NAME -- trino --execute "${query}" > /dev/null
        QUERY_RESULT="SELECT date_diff('millisecond',\"created\",\"end\")/1000.0000, query from system.runtime.queries ORDER BY created DESC OFFSET 1 limit 1;"
        kubectl exec --stdin --tty --namespace $TRINO_NAMESPACE $POD_NAME -- trino --execute "$QUERY_RESULT" >> "./trino_queries/logs/${SF}_${REP}_$ele.log" |& grep "" >> ./trino_queries/results/trino_times_tpch_SF_${SF}_executions_${REP}.txt
        sleep 2
        echo Executed query "${query}"
      done
      drop_revenue_view ${SF}
      drop_tpch_tables ${SF}
    done
  else
  echo "Query file doesn't exist"
  fi
}


stop_trino () {
  kubectl delete namespace $TRINO_NAMESPACE
  kubectl delete namespace $HIVE_NAMESPACE
  kubectl delete persistentvolumeclaim/data-my-hive-postgresql-0
  kubectl delete persistentvolume/nfs-pv-hive-postgresql
}
