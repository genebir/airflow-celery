from airflow.hooks.base import BaseHook
from utils.api import Api
import pandas as pd
import requests as rq

class SubwayHook(BaseHook):
    def __init__(self,
                 subway_name: str = None,
                 *args,
                 **kwargs):
        super().__init__()
        self.url = f"{Api.SUBWAY_API__URL.value}{Api.AUTH_KEY.value}/json/realtimePosition/0/1000/{subway_name}"
        rs = rq.get(self.url).json()
        self.subways = rs['realtimePositionList']
        self.status = rs['errorMessage']

    def transform(self):
        df = pd.DataFrame.from_records(self.subways)
