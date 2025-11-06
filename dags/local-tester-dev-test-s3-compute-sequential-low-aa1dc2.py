from __future__ import annotations
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.exceptions import AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from kubernetes.client import models as k8s
import logging

logger = logging.getLogger(__name__)

default_args = {
    "owner": "local-tester",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "priority_weight": 5,
    "weight_rule": 'absolute'
}

@dag(
    dag_id="local-tester-dev-test-s3-compute-sequential-low-aa1dc2",
    schedule_interval=None,
    default_args=default_args,
    tags=["SEQUENTIAL", "dev-s3-context-type", "dev-s3-context", "dev-s3-compute-dag", "dev-s3-context-dev-s3-compute-dag", "low"],
    catchup=False,
    is_paused_upon_creation=False,
    concurrency=20, 
    max_active_runs=10
)
def dynamic_k8s_dag():
    """
    DAG for sequentially executing Kubernetes Pod tasks based on the runtime configuration.
    """
    @task
    def get_config(**context):
        """
        Fetches configuration from the DAG run context and stores additional settings in XCom.
        """
        dag_run = context["dag_run"]
        if dag_run and dag_run.conf:
            conf = dag_run.conf.get("tasks", "[]")
            additional_config = {
                "node_name": dag_run.conf.get("node_name"),
                "pod_namespace": dag_run.conf.get("pod_namespace"),
                "priority": dag_run.conf.get("priority"),
                "user_name": dag_run.conf.get("user_name"),
                "user_id": dag_run.conf.get("user_id"),
                "memory": dag_run.conf.get("memory"),
                "cpu": dag_run.conf.get("cpu"),
                "container_repository": dag_run.conf.get("container_repository"),
                "fsx_claim_name": dag_run.conf.get("fsx_claim_name"),
                "fsx_mount_path": dag_run.conf.get("fsx_mount_path"),
                "fsx_volume_name": dag_run.conf.get("fsx_volume_name"),
                "parallel_execution": dag_run.conf.get("parallel_execution"),
                "node_selector_type": dag_run.conf.get("node_selector_type", "brain.nodegroup"),
                "node_selector_value": dag_run.conf.get("node_selector_value", "jobs"),
            }
            for key, value in additional_config.items():
                context["ti"].xcom_push(key=key, value=value)
                logger.info(f"Pushed {key}: {value} to XCom")
            return conf
        else:
            logger.warning("No configuration found in the DAG run context.")
            return {}

    
    def check_previous_tasks(context, task_instance):
        """
        Checks the status of all previously executed mapped tasks before executing the current task.
        If any previous task has failed (indicated by a False status), this function will raise
        an AirflowSkipException to skip the execution of the current task.
        """
        current_index = task_instance.map_index
        logger.debug(f'Mapped tasked index : {current_index}')
        if current_index > 0:
            task_id = 'create_kubernetes_pod_task'
            status = context['ti'].xcom_pull(task_ids=task_id, key='task_status')
            logger.debug(f'status : {status}')
            if False in status:
                raise AirflowSkipException(f"Skipping due to failure in {task_id}")

    @task(trigger_rule=TriggerRule.ALL_SUCCESS, max_active_tis_per_dag=1)
    def create_kubernetes_pod_task(task_config, **context):
        """
        Creates and executes a Kubernetes Pod for each configured task.
        """
        if task_config:
            ti = context["ti"]
            check_previous_tasks(context, ti)
            user_name = ti.xcom_pull(key="user_name", task_ids="get_config")
            user_id = ti.xcom_pull(key="user_id", task_ids="get_config")
            memory = ti.xcom_pull(key="memory", task_ids="get_config")
            cpu = ti.xcom_pull(key="cpu", task_ids="get_config")
            fsx_mount_path = ti.xcom_pull(key="fsx_mount_path", task_ids="get_config")
            fsx_volume_name = ti.xcom_pull(key="fsx_volume_name", task_ids="get_config")
            fsx_claim_name = ti.xcom_pull(key="fsx_claim_name", task_ids="get_config")
            priority = ti.xcom_pull(key="priority", task_ids="get_config")
            pod_namespace = ti.xcom_pull(key="pod_namespace", task_ids="get_config")
            node_selector_type = context["ti"].xcom_pull(key="node_selector_type", task_ids="get_config")
            node_selector_value = context["ti"].xcom_pull(key="node_selector_value", task_ids="get_config")
            node_selector = {node_selector_type: node_selector_value}

            if task_config["container_executer"]  == 'python':
                command = (
                    f"adduser --disabled-password --uid {user_id} --allow-bad-names --gecos '' {user_name} && "
                    f"su - {user_name} -c 'source /brain/brain_env/bin/activate && python -u /brain/python_driver.py {task_config['file_path']} {task_config['log_path']}'"
                )
            elif task_config["container_executer"]  == 'rstudio':
                command = (
                    f"adduser --disabled-password --uid {user_id} --allow-bad-names --gecos '' {user_name} && "
                    f"su - {user_name} -c '/brain/r_driver.sh -v {task_config['file_path']} -u {user_name} -z {task_config['log_path']}'"
                )
            else:
                logger.error(f"Unsupported container executor: {task_config['container_executer']}")
                raise ValueError(f"Unsupported container executor: {task_config['container_executer']}")

            logger.info(f"Executing {task_config['container_executer']} program")
            logger.info(f"Command to execute : {command}")

            request_memory = Variable.get("request_memory", default_var="100Mi")
            request_cpu = Variable.get("request_cpu", default_var="20m")
            
            try:
                KubernetesPodOperator(
                    task_id=f"task-{task_config['file_name'].replace('.', '-')}",
                    namespace=pod_namespace,
                    labels={"release": "un-stable"},
                    cmds=["/bin/sh"],
                    arguments=["-c", command],
                    name=f"task-{task_config['file_name'].replace('.', '-')}",
                    get_logs=True,
                    priority_class_name=priority,
                    is_delete_operator_pod=True,
                    image_pull_policy="Always",
                    do_xcom_push=True,
                    in_cluster=True,
                    image=task_config["image"],
                    container_resources = k8s.V1ResourceRequirements(
                        requests={"memory": request_memory, "cpu": request_cpu},
                        limits={"memory": memory, "cpu": cpu}
                    ),
                    volume_mounts=[
                        k8s.V1VolumeMount(mount_path=fsx_mount_path, name=fsx_volume_name)
                    ],
                    volumes=[
                        k8s.V1Volume(
                            name=fsx_volume_name,
                            persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                                claim_name=fsx_claim_name
                            ),
                        )
                    ],
                    node_selector=node_selector,
                ).execute(context)
                ti.xcom_push(key='task_status', value=True)
                logger.info(f"Executed pod for task: {task_config['file_name']}")
            except Exception as e:
                ti.xcom_push(key='task_status', value=False)
                logger.error(f"Task failed: {e}")
                raise

    config = get_config()
    create_tasks = create_kubernetes_pod_task.expand(task_config=config)

    config >> create_tasks


dag_instance = dynamic_k8s_dag()
