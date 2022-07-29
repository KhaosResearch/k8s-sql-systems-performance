# Env variables
SPARK_CHART_REPO="bitnami https://charts.bitnami.com/bitnami"
SPARK_CHART=bitnami/spark
SPARK_CHART_NAME=spark-cluster
NAMESPACE=spark-cluster 
# Load util functions
source ./utils.sh

# Deploy Spark
deploy_spark () {
  # Clear OS caches
  clear_memory

  # Create namespace
  kubectl create namespace $NAMESPACE;

  # Deploy Spark chart into the namespace  
  # helm repo add $SPARK_CHART_REPO;
  # helm repo update ;
  helm install -f $OVERRIDE_VALUES_FOLDER/spark.yml $SPARK_CHART_NAME $SPARK_CHART -n $NAMESPACE;

  echo "Chart $SPARK_CHART_NAME deployed, waiting for pods to be ready...";

  wait_for_all $NAMESPACE
  echo "Pods for $SPARK_CHART_NAME ready.";
}


# Spark UI in http://192.168.213.23:9998/ or http://<worker_node_ip>:9997
start_spark_ui(){
    if [$1=='WORKER'];then
        # Acceso a la interfaz web de un worker (se debe tener el anterior comando en ejecución):
        sudo kubectl port-forward --namespace default pod/spark-cluster-worker-3 9997:8081 --address 0.0.0.0
    else
        # Acceso a la interfaz web general del master:
        sudo kubectl port-forward --namespace default svc/spark-cluster-master-svc 9998:80 --address 0.0.0.0 
    fi
}

# execute_tpch_in_spark <tpch SF> <number of executions>
# Example: execute_tpch_in_spark 100 5
execute_tpch_in_spark () {
  # Load tpch queries
  if source ./spark_queries/tpch.sh; then
    TIMEFORMAT=%R
    # Execute queries
    local i=0
    local j=0
    for ((i = 0; i < ${#SPARK_QUERIES[@]}; i++))
    do
      clear_memory
      echo "${SPARK_QUERIES[$i]};${1}" >> ./spark_queries/spark_times_tpch_SF_${1}_executions_${2}.txt
      for ((j = 0; j < ${2}; j++))
      do
        (time (kubectl exec -ti --namespace $NAMESPACE spark-cluster-worker-0 -- spark-submit --master spark://spark-cluster-master-svc:7077 hdfs://IP:8020/tpch/spark/queries/${SPARK_QUERIES[$i]} ${1} >> "./spark_queries/logs/${SPARK_QUERIES[$i]}_$i.log")) |& grep "" >> ./spark_queries/spark_times_tpch_SF_${1}_executions_${2}.txt
        sleep 2
        echo Executed query ${SPARK_QUERIES[$i]}
      done
    done
  else
  echo "Query file doesn't exists"
  fi
}

stop_spark () {
  kubectl delete namespace $NAMESPACE  
}
