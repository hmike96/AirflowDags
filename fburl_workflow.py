import airflow
from airflow  import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator




default_args = {
    'owner': 'dataloader',
    'depends_on_past': False,
    'start_date': datetime(2019, 6, 2),
    'end_date': datetime(2019, 6, 3),
    'email': ['mike.hewitt@nielsen.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

def get_half_hour(hour, minute):
    halfhour = hour * 2
    if minute is 30:
        halfhour = halfhour + 1
    return halfhour

def get_s3key_location(execution_date, suffix):
    hour = execution_date.hour
    minute = execution_date.minute
    year = execution_date.year
    month = execution_date.month
    day = execution_date.day 
    return "date=" + str(year) + "-" + str(month).zfill(2) + "-" + str(day).zfill(2) + "/hh=" + str(get_half_hour(hour, minute)).zfill(2) + suffix

dag = DAG('fburlworkflow_dag_v2', default_args=default_args, schedule_interval = '*/30 * * * *', user_defined_macros={"get_half_hour": get_half_hour, "get_s3key_location": get_s3key_location})

sensor = S3KeySensor(
    task_id = 's3keysensor',
    poke_interval = 20,
    timeout = 10,
    soft_fail = True,
    wildcard_match=True,
    bucket_key = 's3://useast1-nlsn-cfn-dataload-univloader-dataloader-qa9-nonprod/{{ get_s3key_location(execution_date, "/*/_SUCCESS") }}',
    bucket_name = None,
    aws_conn_id='aws_default',
    executor_config=
                         {
                             "KubernetesExecutor":
                             {
                                 "annotations": 
                                {
                                    "iam.amazonaws.com/role":"arn:aws:iam::407121322241:role/alexk8-test-cluster-s3"
                                }
                            }
                        },
    dag = dag
)
check_file = KubernetesPodOperator(in_cluster=True,
                         namespace='airflow-blue',
                         service_account_name='mike',
                         image="hmike96/checksuccess:0.0.1",
                         cmds=["/bin/sh","-c"],
                         arguments=['sleep 1s; python3 ./s3keycheck.py useast1-nlsn-cfn-dataload-univloader-dataloader-qa9-nonprod {{ get_s3key_location(execution_date, "/_SUCCESS") }}'],
                         # Arguments to the entrypoint. The docker image's CMD is used if this
                         # is not provided. The arguments parameter is templated.
                         # arguments=['s3', 'ls', 'useast1-nlsn-cfn-eventcensus-univloader-dataloader-qa9-nonprod/'],
                         name="checkfile",
                         task_id="checkfile",
                         get_logs=True,
                         is_delete_operator_pod=True,
                         annotations={"iam.amazonaws.com/role":"arn:aws:iam::407121322241:role/alexk8-test-cluster-s3"},
                         dag=dag
                        )

spark = KubernetesPodOperator(in_cluster=True,
                         namespace='airflow-blue',
                         service_account_name='mike',
                         image="hmike96/fburlexample:0.0.2",
                         cmds=["/bin/sh","-c"],
                         arguments=['/opt/spark/bin/spark-submit --master k8s://https://8F2AD77A3A26EE26503AFE149C713239.gr7.us-east-1.eks.amazonaws.com:443 --deploy-mode cluster --name DCR_FBURL  --class com.nielsen.webcensus.fburl.FBUrl --conf spark.executor.instances=10 --conf spark.kubernetes.container.image=hmike96/fburlexample:0.0.2 --conf spark.kubernetes.namespace=airflow-blue --conf spark.kubernetes.authenticate.driver.serviceAccountName=mike --conf "spark.driver.extraClassPath=/opt/app/lib/aws-java-sdk-1.7.4.jar:/opt/app/lib/hadoop-auth-2.7.3.jar:/opt/app/lib/hadoop-aws-2.7.3.jar:/opt/app/lib/hadoop-common-2.7.3.jar:/opt/app/lib/jets3t-0.9.4.jar" --conf spark.kubernetes.driver.annotation.iam.amazonaws.com/role=arn:aws:iam::407121322241:role/alexk8-test-cluster-s3 --conf spark.kubernetes.executor.annotation.iam.amazonaws.com/role=arn:aws:iam::407121322241:role/alexk8-test-cluster-s3  --jars local:///opt/app/lib/aws-java-sdk-1.7.4.jar,local:///opt/app/lib/hadoop-auth-2.7.3.jar,local:///opt/app/lib/hadoop-aws-2.7.3.jar,local:///opt/app/lib/hadoop-common-2.7.3.jar,local:///opt/app/lib/jets3t-0.9.4.jar  local:///opt/app/alextest-0.0.1-SNAPSHOT.jar useast1-nlsn-cfn-eventcensus-univloader-dataloader-qa9-nonprod/hive/dcr_prod.db/dcr_crediting_processed_viewability/dt=2019-06-20/hh=18 useast1-nlsn-cfn-eventcensus-univloader-dataloader-qa9-nonprod/miketest'],
                         # Arguments to the entrypoint. The docker image's CMD is used if this
                         # is not provided. The arguments parameter is templated.
                         # arguments=['s3', 'ls', 'useast1-nlsn-cfn-eventcensus-univloader-dataloader-qa9-nonprod/'],
                         name="fburl",
                         task_id="fburl_spark",
                         get_logs=True,
                         is_delete_operator_pod=True,
                         annotations={"iam.amazonaws.com/role":"arn:aws:iam::407121322241:role/alexk8-test-cluster-s3"},
                         dag=dag
                        )

validate_spark = DummyOperator(task_id='validate', dag=dag)


task_success = DummyOperator(task_id='SUCCESS', 
                             trigger_rule='none_failed', 
                             dag=dag)

task_failed = DummyOperator(task_id='FAILED', 
                             trigger_rule='one_failed', 
                             dag=dag)

sensor >> check_file >> spark >> validate_spark >> [task_success,  task_failed]