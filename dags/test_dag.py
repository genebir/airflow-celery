from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from pendulum import timezone
from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
from utils.api import Api

default_args = {
    'owner': 'airflow',
    'schedule': None,
    'catchup': False,
}

#####################################################################
#
#   함수 선언 영역
#
#####################################################################

def get_url(segment: str) -> dict:
    url = Api.BASE_URL.value + Api.POPULAR_URL.value

    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    links={}
    with webdriver.Remote('http://172.20.0.4:4444/wd/hub',
                          options=options) as driver:
        driver.get(url)
        li = driver.find_element(By.CLASS_NAME, 'list-literacy') \
            .find_elements(By.TAG_NAME, 'ul')[0] \
            .find_elements(By.TAG_NAME, 'li')
        for _ in li:
            seg = _.text
            if seg == segment:
                a = _.find_element(By.TAG_NAME, 'a')
                a.click()
                break
        td = driver.find_elements(By.TAG_NAME, 'td')
        print(td)
        _a = sum(list(filter(lambda x: len(x) > 0, [_.find_elements(By.TAG_NAME, 'a') for _ in td])), [])
        print(_a)
        links[seg] = [_.get_attribute('href') for _ in _a]
    return links

#####################################################################

meta_df = pd.read_csv('/opt/airflow/data/meta/segment.csv', encoding='utf-8')
