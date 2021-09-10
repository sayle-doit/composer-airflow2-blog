# Sayle's Composer Airflow 2.0 Medium Article Code

This is the codebase to be used with the Medium article located at: https://blog.doit-intl.com/running-containers-on-cloud-composer-with-airflow-2-0-23cd86615969

It contains 2 DAGs that will run a Docker container.

The python file airflow_composer_cluster_gke_pod.py contains a DAG (airflow-gke-cluster-dag) that creates a new GKE node pool, run 2 KubernetesPodOperator tasks, and then delete the created node pool.

The python file new_cluster_gke_pod contains a DAG (new-gke-cluster-dag) that creates a new GKE cluster, runs the container on the node pool, and then deletes the GKE cluster.

## Notes
In order to run either of these DAGS you will need to create a new Cloud Composer instance inside of your GCP project and then copy the contents of this repository into your DAGs Google Cloud Storage bucket.

Once that has been completed, refresh your Airflow UI waiting for it to reload the DAGs, and then they will be ready to run.

## DAGs
### airflow-gke-cluster-dag
This DAG will run on the Composer GKE Cluster inside of a newly created node pool.

### Task Definitions
get_project_name_task<br />
Gets the GCP project name.

get_cluster_name_task<br />
Gets the Composer GKE cluster name.

get_zone_task<br />
Gets the name of the zone the Composer GKE cluster is running in.

assign_node_pool_name_to_variable_task<br />
Generates a node pool name and assigns it to an Airflow variable.

create_node_pool_task<br />
Creates the Node Pool in the Composer GKE cluster.

etl_task, etl_task2<br />
User-replaceable tasks that will load up an Ubuntu container that sleeps for 120 seconds and then exits.

delete_node_pool_task<br />
Deletes the created node pool.

delete_node_pool_name_to_variable_task<br />
Deletes the Airflow variable containing the node pool name.

### Environment Variables
NODE_COUNT<br />
The number of nodes to provision inside the node pool. Default value is 3

MACHINE_TYPE<br />
The instance type for instances provisioned inside the node pool. Default value is e2-standard-8<br />
This equates to a VM with 8 vCPUs and 32GB of RAM

SCOPES<br />
The scopes that will be applied to the nodes inside the node pool. Default value is default,cloud-platform

### new-gke-cluster-dag
This DAG will create a new GKE cluster, run the tasks on it, and delete the cluster once they are completed.

### Task Definitions
get_project_task<br />
Gets the GCP project name.

get_zone_task<br />
Gets the name of the zone the Composer GKE cluster is running in.

create_cluster_task<br />
Creates a new GKE cluster.

etl_task, etl_task2<br />
User-replaceable tasks that will load up an Ubuntu container that sleeps for 120 seconds and then exits.

delete_cluster_task<br />
Deletes the GKE cluster created earlier.
