from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
import time
import json
import ndjson


class ProgrammersCrawler:
    driver_path = "/usr/src/chrome/chromedriver"
    options = None
    service = None
    
    def __init__(self):
        self.service = Service(executable_path='./usr/src/chrome/chromedriver.exe')
        self.options = webdriver.ChromeOptions()
        self.options.add_argument('--headless')
        # options.add_argument('window-size=1200x600')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')
        
    
    def crawling_start(self):
        page_href_list = self.crawling_page_list()
        self.crawl_page_content(page_href_list)

    def crawling_page_list(self):
        """
        크롤링 대상 page list 만들기
        :return: 크롤링 대상 page list의 링크
        """
        url = 'https://career.programmers.co.kr/job?page=1'

        browser = webdriver.Chrome(service=self.service,options=self.options)
            
        browser.get(url)

        titles = browser.find_elements(By.CLASS_NAME, "list-position-item")  # class로 가져오기
        href_list = []

        for i, tit in enumerate(titles):
            # print(tit.text)
            body = tit.find_element(By.TAG_NAME, "a")  # 태그로 가져오기
            #print(body.get_attribute("href"))
            href_list.append(body.get_attribute("href"))

        browser.quit()
        return href_list

    def crawl_page_content(self, page_href_list: list):
        #result = pd.DataFrame(index=range(0, 0), columns=['title', 'company', 'task', 'deadline', 'contract_type', 'career', 'location'])
        json_data = []

        for i, href in enumerate(page_href_list):
            browser = webdriver.Chrome(service=self.service,options=self.options)
            browser.get(href)
            title = browser.find_element(By.CLASS_NAME, "KWImVsDeFt2E93NXqAqt").find_element(By.TAG_NAME, "h2").text

            company = browser.find_element(By.CLASS_NAME, "e_ow99Z6WyqEsY3oG3gk").text

            body = browser.find_element(By.CLASS_NAME, "oSd94NeynGy8qiuPFFgg")
            contents = body.text.split('\n')

            try:
                stacks = browser.find_element(By.CLASS_NAME, "section-stacks")

                stack_list = stacks.text.split('\n')[1:]
            except:
                stack_list = []

            data = [['title', title],
                    ['company', company],
                    ['link', href],
                    ['task', contents[1]],
                    ['deadline', contents[3]],
                    ['contract_type', contents[5]],
                    ['career', contents[7]],
                    ['location', contents[9]],
                    ['stacks', stack_list]
                    ]
            temp = json.dumps(dict(data), ensure_ascii=False)
            json_data.append(json.loads(temp))

            browser.close()
            if i > 1:
                break
            time.sleep(2)  # 2초 간격 크롤링


        with open('data.ndjson', 'w', encoding='UTF-8-sig') as f:
            ndjson.dump(json_data, f, ensure_ascii=False)


#ProgrammersCrawler().crawling_start()
