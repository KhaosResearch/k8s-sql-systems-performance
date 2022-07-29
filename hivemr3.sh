# Env variables
MYSQL_CHART=bitnami/mysql
MYSQL_CHART_NAME=mysql-hive

HIVE_CHART_PATH=/home/zriccra/hive-alt/mr3-run/kubernetes/helm/hive/
HIVE_NAMESPACE=hivemr3
HIVE_CHART_NAME=hivemr3
# Load util functions
source ./utils.sh

# Deploy Hive
deploy_hive () {
  # Clear OS caches
  clear_memory

  # Create namespace
  kubectl create namespace $HIVE_NAMESPACE;

  helm install $MYSQL_CHART_NAME $MYSQL_CHART -n $HIVE_NAMESPACE

  wait_for_all $HIVE_NAMESPACE

  MYSQL_PASS=$(kubectl get secret --namespace $HIVE_NAMESPACE $MYSQL_CHART_NAME -o jsonpath="{.data.mysql-root-password}" | base64 --decode)
  sed -i "803s/.*/  <value>$MYSQL_PASS<\/value>/" $HIVE_CHART_PATH/conf/hive-site.xml

  helm install $HIVE_CHART_NAME $HIVE_CHART_PATH -n $HIVE_NAMESPACE

  echo "Chart $HIVE_CHART_NAME deployed, waiting for pods to be ready...";

  wait_for_all $HIVE_NAMESPACE
  
  echo "Pods for $HIVE_CHART_NAME ready.";
}


# create_tpch_tables <tpch SF>
# Example: create_tpch_tables 100
create_tpch_tables () {
  # Load tpch tables
  if source ./hive_queries/tpch-tables.sh; then
    TIMEFORMAT=%R
    POD_NAME=$(kubectl get pods --no-headers -o custom-columns=":metadata.name" -n $HIVE_NAMESPACE | grep hiveserver)
    POD_IP=$(kubectl get pod $POD_NAME --template={{.status.podIP}} -n $HIVE_NAMESPACE)
    # Execute statements
    local i=0
    local j=0
    for ((i = 0; i < ${#HIVE_TABLES[@]}; i++))
    do
      echo "${HIVE_TABLES[$i]};${1}"
      local query=${HIVE_TABLES[$i]//'@'/${1}}
      echo beeline -u "jdbc:hive2://$POD_IP:9852/;;;" -e "${query}" -n zriccra -p zriccra --hiveconf hive.querylog.location=/home/zriccra/hive-alt/mr3-run/kubernetes/hive/hive/run-result/hivemr3-2021-10-25-15-21-15/hive-logs
      beeline -u "jdbc:hive2://$POD_IP:9852/;;;" -e "${query}" -n zriccra -p zriccra --hiveconf hive.querylog.location=/home/zriccra/hive-alt/mr3-run/kubernetes/hive/hive/run-result/hivemr3-2021-10-25-15-21-15/hive-logs
      sleep 2
      echo Executed query ${query}
    done
  else
  echo "Query file doesn't exists"
  fi
}

# create_tpch_views <tpch SF>
# Example: create_tpch_views 100
create_tpch_views () {
  # Load tpch views
  if source ./hive_queries/tpch-create-views.sh; then
    TIMEFORMAT=%R
    POD_NAME=$(kubectl get pods --no-headers -o custom-columns=":metadata.name" -n $HIVE_NAMESPACE | grep hiveserver)
    POD_IP=$(kubectl get pod $POD_NAME --template={{.status.podIP}} -n $HIVE_NAMESPACE)
    # Execute statements
    local i=0
    local j=0
    for ((i = 0; i < ${#HIVE_VIEWS[@]}; i++))
    do
      echo "${HIVE_VIEWS[$i]};${1}"
      local query=${HIVE_VIEWS[$i]//'@'/${1}}
      echo beeline -u "jdbc:hive2://$POD_IP:9852/;;;" -e "${query}" -n zriccra -p zriccra --hiveconf hive.querylog.location=/home/zriccra/hive-alt/mr3-run/kubernetes/hive/hive/run-result/hivemr3-2021-10-25-15$
      beeline -u "jdbc:hive2://$POD_IP:9852/;;;" -e "${query}" -n zriccra -p zriccra --hiveconf hive.querylog.location=/home/zriccra/hive-alt/mr3-run/kubernetes/hive/hive/run-result/hivemr3-2021-10-25-15-21-1$
      sleep 2
      echo Executed query ${query}
    done
  else
  echo "Query file doesn't exists"
  fi
}

# drop_tpch_tables <tpch SF>
# Example: drop_tpch_tables 100
drop_tpch_tables () {
  # Load tpch tables
  if source ./hive_queries/tpch-drop-tables.sh; then
    TIMEFORMAT=%R
    POD_NAME=$(kubectl get pods --no-headers -o custom-columns=":metadata.name" -n $HIVE_NAMESPACE | grep hiveserver)
    POD_IP=$(kubectl get pod $POD_NAME --template={{.status.podIP}} -n $HIVE_NAMESPACE)
    # Execute statements
    local i=0
    local j=0
    for ((i = 0; i < ${#HIVE_TABLES[@]}; i++))
    do
      echo "${HIVE_TABLES[$i]};${1}"
      local query=${HIVE_TABLES[$i]//'@'/${1}}
      echo beeline -u "jdbc:hive2://$POD_IP:9852/;;;" -e "${query}" -n zriccra -p zriccra --hiveconf hive.querylog.location=/home/zriccra/hive-alt/mr3-run/kubernetes/hive/hive/run-result/hivemr3-2021-10-25-15-21-15/hive-logs
      beeline -u "jdbc:hive2://$POD_IP:9852/;;;" -e "${query}" -n zriccra -p zriccra --hiveconf hive.querylog.location=/home/zriccra/hive-alt/mr3-run/kubernetes/hive/hive/run-result/hivemr3-2021-10-25-15-21-15/hive-logs
      sleep 2
      echo Executed query ${query}
    done
  else
  echo "Query file doesn't exists"
  fi
}

# drop_tpch_views <tpch SF>
# Example: drop_tpch_views 100
drop_tpch_views () {
  # Load tpch tables
  if source ./hive_queries/tpch-drop-views.sh; then
    TIMEFORMAT=%R
    POD_NAME=$(kubectl get pods --no-headers -o custom-columns=":metadata.name" -n $HIVE_NAMESPACE | grep hiveserver)
    POD_IP=$(kubectl get pod $POD_NAME --template={{.status.podIP}} -n $HIVE_NAMESPACE)
    # Execute statements
    local i=0
    local j=0
    for ((i = 0; i < ${#HIVE_VIEWS[@]}; i++))
    do
      echo "${HIVE_VIEWS[$i]};${1}"
      local query=${HIVE_VIEWS[$i]//'@'/${1}}
      echo beeline -u "jdbc:hive2://$POD_IP:9852/;;;" -e "${query}" -n zriccra -p zriccra --hiveconf hive.querylog.location=/home/zriccra/hive-alt/mr3-run/kubernetes/hive/hive/run-result/hivemr3-2021-10-25-15$
      beeline -u "jdbc:hive2://$POD_IP:9852/;;;" -e "${query}" -n zriccra -p zriccra --hiveconf hive.querylog.location=/home/zriccra/hive-alt/mr3-run/kubernetes/hive/hive/run-result/hivemr3-2021-10-25-15-21-1$      sleep 2
      echo Executed query ${query}
    done
  else
  echo "Query file doesn't exists"
  fi
}


execute_tpch_hive_test () {
  # Load tpch queries
  if source ./hive_queries/tpch.sh; then
    for ((i = 0; i < ${#HIVE_QUERIES[@]}; i++))
    do
      echo "Inside first loop. i: $i, dollar1: ${1}, dollar2: ${2}"
      for ((j = 0; j < ${2}; j++))
      do
        echo "INSIDE SECOND LOOP. i: $i, j: $j, dollar1: ${1}, dollar2: ${2}"
      done
    done
  fi
}

# execute_tpch_in_hive <tpch SF> <number of executions>
# Example: execute_tpch_in_hive 100 5
execute_tpch_in_hive () {
  # Load tpch queries
  if source ./hive_queries/tpch.sh; then
    TIMEFORMAT=%R
    POD_NAME=$(kubectl get pods --no-headers -o custom-columns=":metadata.name" -n $HIVE_NAMESPACE | grep hiveserver)
    POD_IP=$(kubectl get pod $POD_NAME --template={{.status.podIP}} -n $HIVE_NAMESPACE)
    # Execute queries
    local i=0
    local j=0
    for ((i = 0; i < ${#HIVE_QUERIES[@]}; i++))
    do
      clear_memory
      create_tpch_tables ${1}
      create_tpch_views ${1}
      echo "${HIVE_QUERIES[$i]//'@'/${1}};${1}" >> ./hive_queries/results/hive_times_tpch_SF_${1}_executions_${2}.txt
      for ((j = 0; j < ${2}; j++))
      do
        local query=${HIVE_QUERIES[$i]//'@'/${1}}
        echo -e "\n\nINSIDE SECOND LOOP. i: $i, j: $j, dollar1: ${1}, dollar2: ${2}"   
        echo beeline -u "jdbc:hive2://$POD_IP:9852/;;;" -e "${query}" -n zriccra -p zriccra --hiveconf hive.querylog.location=/home/zriccra/hive-alt/mr3-run/kubernetes/hive/hive/run-result/hivemr3-2021-10-25-15-21-15/hive-logs
        (time (beeline -u "jdbc:hive2://$POD_IP:9852/;;;" -e "${query}" -n zriccra -p zriccra --hiveconf hive.querylog.location=/home/zriccra/hive-alt/mr3-run/kubernetes/hive/hive/run-result/hivemr3-2021-10-25-15-21-15/hive-logs) > "./hive_queries/logs/${1}_${2}_$i.log") |& grep '[0-9]* [a-z]* selected \(.*\)' >> ./hive_queries/results/hive_times_tpch_SF_${1}_executions_${2}.txt
        sleep 2
        echo Executed query "${query}"
      done
      drop_tpch_views ${1}
      drop_tpch_tables ${1}
    done
  else
  echo "Query file doesn't exists"
  fi
}

stop_hive () {
  kubectl delete namespace $HIVE_NAMESPACE
}
