import requests
from bs4 import BeautifulSoup as bs
from bs4 import Comment
import pprint
import os


class UseSupersetApi:
    def __init__(self, username=None, password=None):
        self.s = requests.Session()
        self.base_url = os.environ['SUPERSET_URL']
        self._csrf = self._getCSRF(self.url('login/'))
        self.headers = {'X-CSRFToken': self._csrf, 'Referer': self.url('login/')}
        # note: does not use headers because of flask_wtf.csrf.validate_csrf
        # if data is dict it is used as form and ends up empty but flask_wtf checks if data ...
        self.s.post(self.url('login/'),
                    data={'username': username, 'password': password, 'csrf_token': self._csrf})

    def url(self, url_path):
        return self.base_url + url_path

    def get(self, url_path):
        return self.s.get(self.url(url_path), headers=self.headers)

    def post(self, url_path, data=None, json_data=None, **kwargs):
        kwargs.update({'url': self.url(url_path), 'headers': self.headers})
        if data:
            data['csrf_token'] = self._csrf
            kwargs['data'] = data
        if json_data:
            kwargs['json'] = json_data
        return self.s.post(**kwargs)

    def _getCSRF(self, url_path):
        response = self.s.get(self.base_url)
        soup = bs(response.content, "html.parser")
        for tag in soup.find_all('input', id='csrf_token'):
            csrf_token = tag['value']
        return csrf_token


superset_username = os.environ['SUPERSET_USERNAME']
superset_password = os.environ['SUPERSET_PASSWORD']
superset = UseSupersetApi(superset_username, superset_password)
saved_queries = superset.get(url_path='/savedqueryviewapi/api/read').text
print(saved_queries)
