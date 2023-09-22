from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.support.wait import WebDriverWait
from plugins.utils.api import Api
import pandas as pd

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

def get_url(segment: str) -> dict:
    url = Api.BASE_URL.value + Api.POPULAR_URL.value

    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--timeout=30')
    links={}
    with webdriver.Remote('http://localhost:4444/wd/hub',
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
print(get_url('코로나19'))