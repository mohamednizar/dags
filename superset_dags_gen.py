import requests
from bs4 import BeautifulSoup
import os
import json
import pprint

superset_host = os.environ['SUPERSET_HOST']
superset_username = os.environ['SUPERSET_USERNAME']
superset_password = os.environ['SUPERSET_PASSWORD']

# set up session for auth
s = requests.Session()
base_url = f"http://{superset_host}/"
login_form = s.post(f"{base_url}/login")


# get Cross-Site Request Forgery protection token
soup = BeautifulSoup(login_form.text, 'html.parser')
csrf_token = soup.find('input', {'id': 'csrf_token'})['value']

# login the given session
s.post(login_form, data=dict(username=superset_username, password=superset_password, csrf_token=csrf_token))

# fetch all saved queries
saved_queries = s.get(f"{base_url}/savedqueryviewapi/api/read").text
saved_queries = json.load(saved_queries)

pprint(saved_queries)




