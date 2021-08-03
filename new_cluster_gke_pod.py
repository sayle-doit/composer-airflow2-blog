import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKECreateClusterOperator,
    GKEDeleteClusterOperator,
    GKEStartPodOperator
)

start_date = datetime.datetime.now()
default_args = {
    'start_date': start_date,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=20),
}

# Bash operator commands

# This is a very basic cluster called example-cluster with an initial node count of 1
# In a real environment it is best to define the node_pools value below as defined by
# the google.cloud.container_v1.types.Cluster class on this page:
# https://googleapis.dev/python/container/latest/container_v1/types.html
cluster_name = "example-cluster"
cluster_definition = {
    "name": cluster_name,
    "initial_node_count": 3
}


with DAG("new-gke-cluster-dag", start_date=start_date, params=default_args) as dag:
    get_project_command = "echo $GCP_PROJECT"
    get_zone_command = "echo $COMPOSER_GKE_ZONE"

    # Note due to Composer not exposing some environment variables up to Airflow a BashOperator must be used
    # to pull the environment variables out of the pod the task runs on inside of a GKE cluster.

    # Get the project name from the environment variable
    get_project_task = BashOperator(task_id="get_project_name", bash_command=get_project_command)

    # Get the zone the current Composer GKE cluster lives in from the environment variable
    get_zone_task = BashOperator(task_id="get_region", bash_command=get_zone_command)

    # Create the GKE cluster
    create_cluster_task = GKECreateClusterOperator(
        task_id="create_cluster",
        project_id=get_project_task.output,
        location=get_zone_task.output,
        body=cluster_definition
    )

    # Run the first "ETL task" in a pod on the GKE cluster
    etl_task1 = GKEStartPodOperator(
        task_id="etl_task1",
        project_id=get_project_task.output,
        location=get_zone_task.output,
        cluster_name=cluster_name,
        namespace="default",
        image='gcr.io/gcp-runtimes/ubuntu_18_0_4',
        cmds=["sh", "-c", 'echo \'Sleeping..\'; sleep 120; echo \'Done!\''],
        name="etl_task1_pod",
    )

    # Run the second "ETL task" in a pod on the GKE cluster
    etl_task2 = GKEStartPodOperator(
        task_id="etl_task2",
        project_id=get_project_task.output,
        location=get_zone_task.output,
        cluster_name=cluster_name,
        namespace="default",
        image='gcr.io/gcp-runtimes/ubuntu_18_0_4',
        cmds=["sh", "-c", 'echo \'Sleeping..\'; sleep 120; echo \'Done!\''],
        name="etl_task2_pod",
    )

    # Delete the created GKE cluster
    delete_cluster_task = GKEDeleteClusterOperator(
        task_id="delete_cluster",
        name=cluster_name,
        project_id=get_project_task.output,
        location=get_zone_task.output
    )

    # Define the order the tasks run in otherwise the graph shown in the UI gets really ugly
    # It's not exactly pretty doing it this way, but looks much better and the path is actually visible
    get_project_task >> get_zone_task >> create_cluster_task >> [etl_task1, etl_task2] >> delete_cluster_task

