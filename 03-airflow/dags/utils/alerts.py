from airflow.utils.email import send_email

def task_failure_callback(context):
    task = context['task']
    dag_id = task.dag_id
    task_id = task.task_id
    execution_date = context['execution_date']
    exception = context.get('exception')
    
    subject = f"ERROR: DAG {dag_id} - Task {task_id} falló"
    html_content = f"""
    <h3>Error en el DAG {dag_id}</h3>
    <p>Tarea: {task_id}</p>
    <p>Fecha de ejecución: {execution_date}</p>
    <p>Error: {exception}</p>
    """
    
    send_email(
        to=['admin@example.com'],
        subject=subject,
        html_content=html_content
    )
