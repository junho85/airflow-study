from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from git import Repo
import os
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'graphrag_document_processing',
    default_args=default_args,
    description='Process GraphRAG documents and upload to GitHub',
    schedule_interval=None,
)


def fetch_md_files():
    print("Cloning GraphRAG repository")
    repo_url = "https://github.com/microsoft/graphrag.git"
    local_repo_path = "/tmp/graphrag_repo"

    # Clone the repository
    if os.path.exists(local_repo_path):
        os.system(f"rm -rf {local_repo_path}")
    Repo.clone_from(repo_url, local_repo_path)

    md_files = []
    for root, dirs, files in os.walk(f"{local_repo_path}/docsite"):
        for file in files:
            if file.endswith('.md'):
                full_file_path = os.path.join(root, file)
                relative_file_path = os.path.relpath(full_file_path, local_repo_path)
                with open(full_file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                md_files.append((relative_file_path, content))

    return md_files


def process_md_files(**kwargs):
    ti = kwargs['ti']
    md_files = ti.xcom_pull(task_ids='fetch_md_files')

    data = []
    for file_path, content in md_files:
        data.append({
            'text': content,
            'source': file_path
        })

    df = pd.DataFrame(data)
    df.to_csv('/tmp/graphrag_doc.csv', index=False)


def upload_to_github(**kwargs):
    repo_url = "git@github.com:junho85/graphrag-input.git"
    local_repo_path = "/tmp/graphrag-input"

    # Clone the repository
    if os.path.exists(local_repo_path):
        os.system(f"rm -rf {local_repo_path}")
    Repo.clone_from(repo_url, local_repo_path)

    # Copy the CSV file
    os.makedirs(f"{local_repo_path}/graphrag_doc", exist_ok=True)
    os.system(f"cp /tmp/graphrag_doc.csv {local_repo_path}/graphrag_doc/")

    # Commit and push changes
    repo = Repo(local_repo_path)
    repo.git.add(all=True)
    repo.index.commit("Update GraphRAG document data")
    origin = repo.remote(name='origin')
    origin.push()


with dag:
    fetch_task = PythonOperator(
        task_id='fetch_md_files',
        python_callable=fetch_md_files,
    )

    process_task = PythonOperator(
        task_id='process_md_files',
        python_callable=process_md_files,
    )

    upload_task = PythonOperator(
        task_id='upload_to_github',
        python_callable=upload_to_github,
    )

    fetch_task >> process_task >> upload_task