# Import necessary modules
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import re
import json
import subprocess

# Define functions for each task

def extract_data():
    """Extracts titles and descriptions from specified web pages."""
    def fetch_data(url):
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        articles = []
        for article in soup.find_all('article'):
            title = article.find('h2').get_text() if article.find('h2') else 'No title'
            description = article.find('p').get_text() if article.find('p') else 'No description'
            articles.append({'title': title, 'description': description})
        return articles

    dawn_articles = fetch_data('https://www.dawn.com')
    bbc_articles = fetch_data('https://www.bbc.com')
    return dawn_articles + bbc_articles

def transform_data(ti):
    """Cleans and preprocesses the extracted text data."""
    articles = ti.xcom_pull(task_ids='extract_data')
    def clean_text(text):
        text = re.sub('<[^<]+?>', '', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    for article in articles:
        article['title'] = clean_text(article['title'])
        article['description'] = clean_text(article['description'])
    return articles

def save_and_version_control(ti):
    """Saves processed data to a file and uses DVC to version control the data."""
    articles = ti.xcom_pull(task_ids='transform_data')
    filename = 'articles.json'
    with open(filename, 'w') as f:
        json.dump(articles, f)

    # DVC and Git commands to version control the data
    subprocess.run(['dvc', 'add', filename], check=True)
    subprocess.run(['git', 'add', filename + '.dvc'], check=True)
    subprocess.run(['git', 'commit', '-m', 'Update data'], check=True)
    subprocess.run(['dvc', 'push'], check=True)
    subprocess.run(['git', 'push'], check=True)

# Define the DAG

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 13),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'complete_data_management_dag',
    default_args=default_args,
    description='A simple DAG for data extraction, transformation, and version control',
    schedule_interval=timedelta(days=1),
)

# Define the tasks

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

save_version_task = PythonOperator(
    task_id='save_and_version_control',
    python_callable=save_and_version_control,
    provide_context=True,
    dag=dag,
)

# Set task dependencies

extract_task >> transform_task >> save_version_task
