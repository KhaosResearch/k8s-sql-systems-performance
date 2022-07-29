OVERRIDE_VALUES_FOLDER=override_values
NODES="IP_1 IP_2 ... IP_n"

clear_memory () {
  # Run "echo 3 > /proc/sys/vm/drop_caches" in all nodes
  for node in $NODES
  do
    ssh -q $node "echo 3 > sudo /proc/sys/vm/drop_caches"
  done
}


wait_for_pod () {
  kubectl wait --timeout=-1s --for=condition=Ready pod/$1 -n $2
}

wait_for_all () {
  kubectl wait --timeout=-1s --for=condition=Ready pods --all -n $1
}

# Get a list of POD names. Example usage: $ pods_by_keyword spark-cluster spark-cluster
pods_by_keyword () {
  local chart_name=${1}
  local namespace=${2:-default}
  kubectl get pods -n $namespace | grep $chart_name | cut -d " " -f 1 -
}

# Delete all from namespace. Example usage: k8s_delete <namespace>
function k8s_delete {
  local chart_name=${1}
  local namespace=${2}
  helm uninstall $chart_name -n $namespace
  
  if [ "$namespace" == "default" ]
  then
    echo "WARNING: Don't try to delete all from default namespace!!"
    return 0
  fi

  for i in $(kubectl api-resources --verbs=list --namespaced -o name | grep -v "events.events.k8s.io" | grep -v "events" | sort | uniq)
  do
    echo "Resource:" $i
    kubectl -n $namespace delete --ignore-not-found $i --all
  done
  
  kubectl delete namespace/$namespace ;
  kubectl delete event --all
}

all_permissions_hdfs(){
  hdfs dfs -chown -R nobody:nogroup /$1
  hdfs dfs -chmod -R 777 /$1
}

start_grafana_ui(){
  GRAFANA_POD=$(kubectl get pods --template '{{range .items}}{{.metadata.name}}{{"\n"}}{{end}}' | grep grafana);
  kubectl port-forward --namespace default pod/$GRAFANA_POD 9991:3000 --address 0.0.0.0; 
}
