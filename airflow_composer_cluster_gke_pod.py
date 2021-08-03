import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKEStartPodOperator
)


start_date = datetime.datetime.now()
default_args = {
    'start_date': start_date,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=1),
}


with DAG("airflow-gke-cluster-dag", start_date=start_date, params=default_args) as dag:
    # This value can be customized to whatever format is preferred for the node pool name
    # Default node pool naming format is <cluster name>-node-pool-<execution_date>
    # This will be reused throughout a number of commands
    generate_node_pool_command = "$COMPOSER_ENVIRONMENT-node-pool-$(echo {{ ts_nodash }} | awk '{print tolower($0)}')"

    # These commands will create and delete airflow variables
    put_node_pool_name_variable_command = "airflow variables set node_pool " + generate_node_pool_command
    delete_node_pool_name_variable_command = "airflow variables delete node_pool"

    create_node_pool_command = """
        # Set some environment variables in case they were not set already
        [ -z "${NODE_COUNT}" ] && NODE_COUNT=3
        [ -z "${MACHINE_TYPE}" ] && MACHINE_TYPE=e2-standard-8
        [ -z "${SCOPES}" ] && SCOPES=default,cloud-platform

        # Generate node-pool name 
        NODE_POOL=""" + generate_node_pool_command + """
        gcloud container node-pools create "$NODE_POOL" --project $GCP_PROJECT --cluster $COMPOSER_GKE_NAME \
        --num-nodes "$NODE_COUNT" --zone $COMPOSER_GKE_ZONE --machine-type $MACHINE_TYPE --scopes $SCOPES \
        --enable-autoupgrade
        """

    delete_node_pools_command = """
        gcloud container node-pools delete \"""" + generate_node_pool_command + """\" \
        --zone $COMPOSER_GKE_ZONE --cluster $COMPOSER_GKE_NAME --quiet
        """

    # Since Composer does not expose some environment variables to Airflow they must be pulled from the environment
    # itself through the use of a BashOperator. These are the commands to pull them.
    get_project_name_command = "echo $GCP_PROJECT"
    get_cluster_name_command = "echo $COMPOSER_GKE_NAME"
    get_cluster_location_command = "echo $COMPOSER_GKE_ZONE"

    # Tasks definitions
    assign_node_pool_name_to_variable_task = BashOperator(
        task_id="assign_node_pool_name_variable",
        bash_command=put_node_pool_name_variable_command
    )
    delete_node_pool_name_to_variable_task = BashOperator(
        task_id="delete_node_pool_name_variable",
        bash_command=delete_node_pool_name_variable_command,
        trigger_rule='all_done' # Always run even if a failure occurs to remove the variable created
    )

    create_node_pool_task = BashOperator(
        task_id="create_node_pool",
        bash_command=create_node_pool_command
    )

    delete_node_pool_task = BashOperator(
        task_id="delete_node_pool",
        bash_command=delete_node_pools_command,
        trigger_rule='all_done'  # Always run even if failures so the node pool is deleted
    )

    # Get the project name from the environment variable
    get_project_name_task = BashOperator(task_id="get_project_name", bash_command=get_project_name_command)

    # Get the name of the GKE cluster that Composer is currently using
    get_cluster_name_task = BashOperator(task_id="get_cluster_name", bash_command=get_cluster_name_command)

    # Get the zone the current Composer GKE cluster lives in from the environment variable
    get_zone_task = BashOperator(task_id="get_region", bash_command=get_cluster_location_command)

    # Run the first "ETL task" in a pod on the GKE cluster
    etl_task = GKEStartPodOperator(
        task_id="etl_task",
        project_id=get_project_name_task.output,
        location=get_zone_task.output,
        cluster_name=get_cluster_name_task.output,
        namespace="default",
        image='gcr.io/gcp-runtimes/ubuntu_18_0_4',
        cmds=["sh", "-c", 'echo \'Sleeping..\'; sleep 120; echo \'Done!\''],
        name="etl_task1",
        startup_timeout_seconds=720,
        # affinity allows you to constrain which nodes your pod is eligible to
        # be scheduled on, based on labels on the node. In this case, if the
        # label 'cloud.google.com/gke-nodepool' with value
        # 'nodepool-label-value' or 'nodepool-label-value2' is not found on any
        # nodes, it will fail to schedule.
        affinity={
            'nodeAffinity': {
                # requiredDuringSchedulingIgnoredDuringExecution means in order
                # for a pod to be scheduled on a node, the node must have the
                # specified labels. However, if labels on a node change at
                # runtime such that the affinity rules on a pod are no longer
                # met, the pod will still continue to run on the node.
                'requiredDuringSchedulingIgnoredDuringExecution': {
                    'nodeSelectorTerms': [{
                        'matchExpressions': [{
                            # When nodepools are created in Google Kubernetes
                            # Engine, the nodes inside of that nodepool are
                            # automatically assigned the label
                            # 'cloud.google.com/gke-nodepool' with the value of
                            # the nodepool's name.
                            'key': 'cloud.google.com/gke-nodepool',
                            'operator': 'In',
                            # The label key's value that pods can be scheduled
                            # on.
                            # In this case it will execute the command on the node
                            # pool created by the Airflow bash operator.
                            'values': [
                                # Note this must be a variable, it cannot be the output of a task in here
                                Variable.get("node_pool", default_var="node_pool")
                            ]
                        }]
                    }]
                }
            }
        })

    # Run the first "ETL task" in a pod on the GKE cluster
    etl_task2 = GKEStartPodOperator(
        task_id="etl_task2",
        project_id=get_project_name_task.output,
        location=get_zone_task.output,
        cluster_name=get_cluster_name_task.output,
        namespace="default",
        image='gcr.io/gcp-runtimes/ubuntu_18_0_4',
        cmds=["sh", "-c", 'echo \'Sleeping..\'; sleep 120; echo \'Done!\''],
        name="etl_task2",
        startup_timeout_seconds=720,
        # affinity allows you to constrain which nodes your pod is eligible to
        # be scheduled on, based on labels on the node. In this case, if the
        # label 'cloud.google.com/gke-nodepool' with value
        # 'nodepool-label-value' or 'nodepool-label-value2' is not found on any
        # nodes, it will fail to schedule.
        affinity={
            'nodeAffinity': {
                # requiredDuringSchedulingIgnoredDuringExecution means in order
                # for a pod to be scheduled on a node, the node must have the
                # specified labels. However, if labels on a node change at
                # runtime such that the affinity rules on a pod are no longer
                # met, the pod will still continue to run on the node.
                'requiredDuringSchedulingIgnoredDuringExecution': {
                    'nodeSelectorTerms': [{
                        'matchExpressions': [{
                            # When nodepools are created in Google Kubernetes
                            # Engine, the nodes inside of that nodepool are
                            # automatically assigned the label
                            # 'cloud.google.com/gke-nodepool' with the value of
                            # the nodepool's name.
                            'key': 'cloud.google.com/gke-nodepool',
                            'operator': 'In',
                            # The label key's value that pods can be scheduled
                            # on.
                            # In this case it will execute the command on the node
                            # pool created by the Airflow bash operator.
                            'values': [
                                # Note this must be a variable, it cannot be the output of a task in here
                                Variable.get("node_pool", default_var="node_pool")
                            ]
                        }]
                    }]
                }
            }
        })

    # Tasks order
    [get_project_name_task, get_cluster_name_task, get_zone_task] >> \
        assign_node_pool_name_to_variable_task >>\
        create_node_pool_task >>\
        [etl_task, etl_task2] >>\
        delete_node_pool_task >>\
        delete_node_pool_name_to_variable_task

