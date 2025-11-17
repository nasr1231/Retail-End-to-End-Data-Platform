from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def print_task_1():
    print("Welcome to the fight club")

def print_task_2():
    print("""
          The rules:
            1. You do not talk about Fight Club.
            2. You do NOT talk about Fight Club.
            3. If a fighter says "Stop," goes limp, or taps out, the fight is over.
            4. Only two guys to a fight.
            5. One fight at a time.
            6. No shirts, no shoes.
            7. Fights will go on as long as they have to.
            8. If this is your first time at Fight Club, you have to fight.
          """)

def print_task_3():
    print("I'm marla singer")

def print_task_4():
    print("البطل مات")
    
default_args = {
    "owner": "finance_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

@dag(
    dag_id="stock_market_ELT_Pipeline",
    start_date=datetime(2025, 10, 1),
    schedule_interval='@daily',    
    catchup=False,
    default_args=default_args
)


def fight_club():

    task_1 = PythonOperator(
        task_id="print_welcome_message",
        python_callable=print_task_1
    )    
    
    task_2 = PythonOperator(
        task_id="print_fight_club_rules",
        python_callable=print_task_2
    )

    task_3 = PythonOperator(
        task_id="print_marla_singer",
        python_callable=print_task_3
    )
    
    task_4 = PythonOperator(
        task_id="final_task",
        python_callable=print_task_4
    )
    
    task_1 >> [task_2, task_3] >> task_4
fight_club()