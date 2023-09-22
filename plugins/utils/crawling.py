from selenium import webdriver
from selenium.webdriver.common.by import By
# from utils.api import Api



def get_urls(segment: str) -> dict:
    url = Api.BASE_URL.value + Api.POPULAR_URL.value

    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--timeout=30')
    links = {}
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

"""
options.add_experimental_option("prefs", {
  "download.default_directory": r"/Users/akamikang/developer/pythonworkspace/stock",
  "download.prompt_for_download": False,
  "download.directory_upgrade": True,
  "safebrowsing.enabled": True
})
"""

def dl_data(url: str):
    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--timeout=30')
    options.add_argument('--disable-popup-blocking')
    options.add_argument('--disable-extensions')

    options.add_experimental_option("prefs", {
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })
    with webdriver.Remote('http://localhost:4444', options=options) as driver:
        driver.get(url)
        from time import sleep
        sleep(10)
        driver.implicitly_wait(10)
        dl_btn = driver.find_element(By.XPATH, '//*[@id="btnCsv"]/span')
        dl_btn.click()
        sleep(10)





dl_data('http://data.seoul.go.kr/dataList/OA-15244/S/1/datasetView.do')