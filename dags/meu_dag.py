from airflow.models import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
import pendulum


with DAG(
    dag_id="primeira_dag",
    catchup=False,
    start_date=pendulum.now("UTC").subtract(days=1), #start date no dia anterior
    schedule="@daily", # intervalo de agendamento
    tags=["dag-1"]
) as minha_dag:

    task_1 = EmptyOperator(task_id="task_1")
    task_2 = EmptyOperator(task_id="task_2")
    task_3 = EmptyOperator(task_id="task_3")

    task_4 = BashOperator(
        task_id = "cria_pasta",
        bash_command = 'mkdir -p "/home/paty/Documents/Codes/airflow_alura/pasta"'
    )

    task_1 >> [task_2, task_3]
    task_3 >> task_4