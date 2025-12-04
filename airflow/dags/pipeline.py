from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# Default args
default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=30),
}


@dag(
    dag_id="E_commerce_ELT_Pipeline_v2",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["ELT", "Electronics", "DBT", "S3"],
    description="Ingests Electronics data from source to S3 and transforms via DBT",
    max_active_runs=1
)
def E_commerce_ELT_Pipeline():

    # DBT paths
    DBT_PROFILES_DIR = '/opt/airflow/electronics_data/dbt'
    DBT_PROJECT_DIR = '/opt/airflow/electronics_data'

    # Start marker
    start = EmptyOperator(task_id="start_pipeline")

    # Test DBT connection
    test_dbt_resources = BashOperator(
        task_id="dbt_test_connection",
        bash_command=f'dbt debug --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR}'
    )

    # Run external tables
    run_external_tables = BashOperator(
        task_id='run_external_tables',
        bash_command=f'dbt run --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR} --models tag:external',
        doc_md="Creates external tables from S3 data sources"
    )

    # Run silver layer transformations
    run_silver_layer = BashOperator(
        task_id='run_silver_layer',
        bash_command=f'dbt run --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR} --models tag:silver',
        doc_md="Transforms bronze data into silver layer"
    )

    # Test silver layer
    test_silver_layer = BashOperator(
        task_id='test_silver_layer',
        bash_command=f'dbt test --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR} --models tag:silver',
        doc_md="Validates silver layer data quality"
    )

    # Dimension tables task group
    with TaskGroup('dimension_tables', tooltip="Load dimension tables") as dim_tables:

        dimension_models = [
            'dim_customer',
            'dim_date',
            'dim_exchange_rates',
            'dim_product_info',
            'dim_stores'
        ]

        for model in dimension_models:
            dbt_run = BashOperator(
                task_id=f'run_{model}',
                bash_command=f'dbt run --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR} --models {model}',
                doc_md=f"Loads {model} dimension table"
            )

            dbt_test = BashOperator(
                task_id=f'test_{model}',
                bash_command=f'dbt test --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR} --models {model}',
                doc_md=f"Validates {model} data quality"
            )

            dbt_run >> dbt_test

    # Fact tables task group
    with TaskGroup('fact_tables', tooltip="Load fact tables") as fact_tables:

        fact_models = ['fact_sales', 'fact_product_sales']

        for f_model in fact_models:
            dbt_run_fact = BashOperator(
                task_id=f'run_{f_model}',
                bash_command=f'dbt run --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR} --models {f_model}',
                doc_md=f"Loads {f_model} fact table"
            )

            dbt_test_fact = BashOperator(
                task_id=f'test_{f_model}',
                bash_command=f'dbt test --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR} --models {f_model}',
                doc_md=f"Validates {f_model} data quality"
            )

            dbt_run_fact >> dbt_test_fact

    # Generate DBT documentation
    generate_dbt_docs = BashOperator(
        task_id="generate_dbt_docs",
        bash_command=f'dbt docs generate --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR}',
        doc_md="Generates DBT documentation for the data models",
        trigger_rule="all_done"
    )

    @task(task_id="data_quality_summary", trigger_rule="all_done")
    def data_quality_summary(**context):
        """
        Summarizes the pipeline run and sends metrics
        """
        from airflow.models import TaskInstance
        from airflow.utils.state import TaskInstanceState

        dag_run = context['dag_run']

        try:
            # Query all task instances for this DAG run
            # Using the session from context (Airflow 2.x way)
            task_instances = dag_run.get_task_instances()

            # Alternative method if above doesn't work
            if not task_instances:
                from airflow.settings import Session
                session = Session()
                task_instances = session.query(TaskInstance).filter(
                    TaskInstance.dag_id == dag_run.dag_id,
                    TaskInstance.run_id == dag_run.run_id
                ).all()
                session.close()

            # Count by state
            failed_tasks = [
                ti.task_id for ti in task_instances if ti.state == TaskInstanceState.FAILED]
            success_tasks = [
                ti.task_id for ti in task_instances if ti.state == TaskInstanceState.SUCCESS]
            running_tasks = [
                ti.task_id for ti in task_instances if ti.state == TaskInstanceState.RUNNING]
            skipped_tasks = [
                ti.task_id for ti in task_instances if ti.state == TaskInstanceState.SKIPPED]

            # Get run information
            run_id = dag_run.run_id
            logical_date = str(dag_run.logical_date)

            # Build summary
            summary = {
                "dag_id": dag_run.dag_id,
                "dag_run_id": run_id,
                "logical_date": logical_date,
                "total_tasks": len(task_instances),
                "successful_tasks": len(success_tasks),
                "failed_tasks": len(failed_tasks),
                "running_tasks": len(running_tasks),
                "skipped_tasks": len(skipped_tasks),
                "failed_task_list": failed_tasks,
                # Show first 10 successful tasks
                "success_task_list": success_tasks[:10],
            }

            # Log detailed summary
            logger.info("=" * 60)
            logger.info("PIPELINE EXECUTION SUMMARY")
            logger.info("=" * 60)
            logger.info(f"DAG ID: {summary['dag_id']}")
            logger.info(f"Run ID: {summary['dag_run_id']}")
            logger.info(f"Logical Date: {summary['logical_date']}")
            logger.info(f"Total Tasks: {summary['total_tasks']}")
            logger.info(f"✓ Successful: {summary['successful_tasks']}")
            logger.info(f"✗ Failed: {summary['failed_tasks']}")
            logger.info(f"⊙ Running: {summary['running_tasks']}")
            logger.info(f"⊘ Skipped: {summary['skipped_tasks']}")

            if failed_tasks:
                logger.error(f"Failed Tasks: {', '.join(failed_tasks)}")
            else:
                logger.info("🎉 All tasks completed successfully!")

            logger.info("=" * 60)

            # Return summary for XCom
            return summary

        except Exception as e:
            logger.error(f"Error generating quality summary: {e}")
            import traceback
            logger.error(traceback.format_exc())

            # Return error summary
            return {
                "error": str(e),
                "dag_run_id": dag_run.run_id if dag_run else "unknown",
                "total_tasks": 0,
                "successful_tasks": 0,
                "failed_tasks": 0
            }

    quality_summary = data_quality_summary()

    # Pipeline complete marker
    pipeline_complete = EmptyOperator(
        task_id="pipeline_complete",
        trigger_rule="all_success"
    )

    # Dependencies
    start >> test_dbt_resources
    test_dbt_resources >> run_external_tables
    run_external_tables >> run_silver_layer
    run_silver_layer >> test_silver_layer
    test_silver_layer >> dim_tables
    dim_tables >> fact_tables
    fact_tables >> generate_dbt_docs
    generate_dbt_docs >> quality_summary
    quality_summary >> pipeline_complete


# Instantiate the DAG
dag_instance = E_commerce_ELT_Pipeline()
