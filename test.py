import json
from elasticsearch import Elasticsearch, ElasticsearchException, NotFoundError
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.support.wait import WebDriverWait
# from plugins.utils.api import DefaultApi
import pandas as pd
import re
import requests as rq

# def get_url():
#     url = Api.BASE_URL.value + Api.POPULAR_URL.value
#
#     options = webdriver.ChromeOptions()
#     options.add_argument('--no-sandbox')
#     options.add_argument('--disable-dev-shm-usage')
#     links={}
#     with webdriver.Remote('http://localhost:4444/wd/hub',
#                           options=options) as driver:
#         driver.get(url)
#         li = driver.find_element(By.CLASS_NAME, 'list-literacy') \
#             .find_elements(By.TAG_NAME, 'ul')[0] \
#             .find_elements(By.TAG_NAME, 'li')
#         for _ in li:
#             seg = _.text
#             a = _.find_element(By.TAG_NAME, 'a')
#             a.click()
#             td = driver.find_elements(By.TAG_NAME, 'td')
#             print(td)
#             _a = sum(list(filter(lambda x: len(x) > 0, [_.find_elements(By.TAG_NAME, 'a') for _ in td])), [])
#             print(_a)
#             links[seg] = [_.get_attribute('href') for _ in _a]
#             driver.back()
#     return links
#
# print(get_url())
#
# def get_url(segment: str) -> dict:
#     url = DefaultApi.BASE_URL.value + DefaultApi.POPULAR_URL.value
#
#     options = webdriver.ChromeOptions()
#     options.add_argument('--no-sandbox')
#     options.add_argument('--disable-dev-shm-usage')
#     options.add_argument('--timeout=30')
#     links={}
#     with webdriver.Remote('http://localhost:4444/wd/hub',
#                           options=options) as driver:
#         driver.get(url)
#         li = driver.find_element(By.CLASS_NAME, 'list-literacy') \
#             .find_elements(By.TAG_NAME, 'ul')[0] \
#             .find_elements(By.TAG_NAME, 'li')
#         for _ in li:
#             seg = _.text
#             if seg == segment:
#                 a = _.find_element(By.TAG_NAME, 'a')
#                 a.click()
#                 break
#         td = driver.find_elements(By.TAG_NAME, 'td')
#         print(td)
#         _a = sum(list(filter(lambda x: len(x) > 0, [_.find_elements(By.TAG_NAME, 'a') for _ in td])), [])
#         print(_a)
#         links[seg] = [_.get_attribute('href') for _ in _a]
#     return links
# print(get_url('코로나19'))


elasticsearch_url = 'http://genebir:@dua7423780@34.64.143.212:9200'
# index_url = f'{elasticsearch_url}/{index_nm}/_search?size=10000'
# index_url = f'{elasticsearch_url}/{index_nm}/_search?size=10000&q=날짜:2021-01-01'
# index_url = f'{elasticsearch_url}/{index_nm}/_search?size=10000&q=날짜:2021-01-01&sort=날짜:desc'
# index_url = f'{elasticsearch_url}/{index_nm}/_search?size=10000&q=날짜:2021-01-01&sort=날짜:desc&from=0'

es = Elasticsearch(elasticsearch_url)
file = open('./dags/elasticsearch/_mapping.json')
mapping = json.load(file)['CycleRentUseMonth']
index_nm = re.sub(r'(?<!^)(?=[A-Z])', '_', 'CycleRentUseMonth').lower()
def create_or_update(es: Elasticsearch,
                     index_nm: str,
                     mapping: dict
                     ):
    try:
        rs = es.indices.get(index=index_nm)
        current_mapping = rs[index_nm]['mappings']['properties']
        for key, value in mapping['properties'].items():
            if key not in current_mapping:
                current_mapping[key] = value
        es.indices.put_mapping(index=index_nm, body=current_mapping)
    except NotFoundError as e:
        es.indices.create(index=index_nm, body=mapping)

create_or_update(es)


file.close()