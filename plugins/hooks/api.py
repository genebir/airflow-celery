from airflow.hooks.base import BaseHook
from airflow.models.variable import Variable
from typing import Dict, Any, List
import json
import requests as rq
class ApiHook(BaseHook):

    def __init__(self,
                 url: str = '',
                 start_index: int = 1,
                 end_index: int = 1,
                 service: str = '',
                 date: str = '',
                 key: str = '',
                 data_type: str = 'json',
                 ):
        super().__init__()
        self.start_index = start_index
        self.end_index = end_index
        self.service = service
        self.date = date
        self.key = key
        self.data_type = data_type
        self.auth_key = Variable.get('api_auth_key')
        if url == '':
            self.url = f'http://openapi.seoul.go.kr:8088/{self.auth_key}/{self.data_type}/{self.service}/{self.start_index}/{self.end_index}/{self.date}'
        else:
            self.url = url

    def get_data(self,
                 **context):
        if self.end_index < 1000:
            print('case 1')
            data = rq.get(url=self.url)
            try:
                return data.json()[self.key]
            except KeyError:
                return data.json()
        else:
            print('case 2')
            result: dict[str, dict[Any, Any] | list[Any] | Any] = {'RESULT': {}, 'row': []}
            flag = True
            try:
                while flag:
                    print(self.start_index, self.end_index)
                    data = rq.get(
                        url=f'http://openapi.seoul.go.kr:8088/{self.auth_key}/{self.data_type}/{self.service}/{self.start_index}/{self.end_index}/{self.date}'
                    )
                    result['RESULT'] = data.json()[self.key]['RESULT']
                    result['row'] = result['row'] + data.json()[self.key]['row']
                    flag = len(data.json()[self.key]['row']) == 1000
                    self.start_index += 1000
                    self.end_index += 1000
                return result
            except KeyError:
                result = rq.get(url=self.url).json()
                return result