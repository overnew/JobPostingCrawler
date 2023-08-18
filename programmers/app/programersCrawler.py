from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
import time
import json
import ndjson
import boto3
import os


class ProgrammersCrawler:
    driver_path = "/usr/src/chrome/chromedriver"
    options = None
    service = None

    next_href_list = []
    check_href_list = []
    table = None
    check_list_size = 10
    
    def __init__(self):
        #for selenium
        self.service = Service(executable_path='./usr/src/chrome/chromedriver.exe')
        self.options = webdriver.ChromeOptions()
        self.options.add_argument('--headless')
        # options.add_argument('window-size=1200x600')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')

        dynamodb = boto3.resource(
            'dynamodb',
            aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
        )
        table_name = '001_crawler_metadata'  # 테이블 이름
        self.table = dynamodb.Table(table_name)

        self.get_saved_href_list()

    def get_saved_href_list(self):
        temp = self.table.scan()['Items']
        for i, row in enumerate(temp):
            self.check_href_list.append(row['store_id'])
        print(self.check_href_list)

    def save_href_list(self):
        for i, href in enumerate(self.check_href_list):
            self.table.delete_item(
                Key={'store_id': href}
            )

        for i, href in enumerate(self.next_href_list):
            self.table.put_item(
                Item={
                    'store_id': href
                })
    
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
            browser = webdriver.Chrome()
            browser.get(href)
            title = browser.find_element(By.CLASS_NAME, "KWImVsDeFt2E93NXqAqt").find_element(By.TAG_NAME, "h2").text

            company = browser.find_element(By.CLASS_NAME, "e_ow99Z6WyqEsY3oG3gk").text

            body = browser.find_element(By.CLASS_NAME, "oSd94NeynGy8qiuPFFgg")
            contents = body.text.split('\n')

            content_body = browser.find_element(By.CLASS_NAME, "yO7TZRtCO7sznD0Csuw_").text

            try:
                stacks = browser.find_element(By.CLASS_NAME, "section-stacks")

                stack_list = stacks.text.split('\n')[1:]
            except:
                stack_list = []

            content_list = []
            idx = 0
            while len(contents) > idx:
                try:
                    content_list.append([contents[idx], contents[idx + 1]])
                except:
                    break
                idx = idx + 2

            data = [['title', title],
                    ['company', company],
                    # ['task', contents[1]],
                    # ['deadline', contents[3]],
                    # ['contract_type', contents[5]],
                    # ['career', contents[7]],
                    # ['location', contents[9]],
                    ['stacks', stack_list],
                    ['body', content_body]
                    ]

            data.extend(content_list)
            print(data)
            temp = json.dumps(dict(data), ensure_ascii=False)
            # json.dump(json.dumps(dict(data), ensure_ascii=False), f, ensure_ascii=False, indent=4)
            json_data.append(json.loads(temp))

            browser.close()
            # if i > 1:
            #     break
            time.sleep(1)  # 2초 간격 크롤링

        with open('data.ndjson', 'w', encoding='UTF-8-sig') as f:
            ndjson.dump(json_data, f, ensure_ascii=False)

        self.save_href_list()


#ProgrammersCrawler().crawling_start()
