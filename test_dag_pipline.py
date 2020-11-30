import requests
import os
import json
import pprint
from bs4 import BeautifulSoup
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.mysql_operator import MySqlOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.utils.dates import days_ago
import logging

superset_host = os.environ['SUPERSET_HOST']
superset_username = os.environ['SUPERSET_USERNAME']
superset_password = os.environ['SUPERSET_PASSWORD']
superset_port = os.environ['SUPERSET_PORT']

# set up session for auth
s = requests.Session()
base_url = f"http://{superset_host}:{superset_port}/"
login_form = s.post(f"{base_url}/login")
logging.info(login_form)

# get Cross-Site Request Forgery protection token
soup = BeautifulSoup(login_form.text, 'html.parser')
# csrf_token = soup.find('input', {'id': 'csrf_token'})['value']

# login the given session
s.post(login_form, data=dict(username=superset_username, password=superset_password))

# fetch all saved queries
saved_queries = s.get(f"{base_url}/savedqueryviewapi/api/read").text

