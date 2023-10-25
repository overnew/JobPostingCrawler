from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import json
import ndjson
import boto3
import os
import re

from datetime import datetime
from datetime import date as dt


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
        #self.service = Service(executable_path='./usr/src/chrome/chromedriver.exe')
        #self.service = Service()
        self.options = webdriver.ChromeOptions()
        self.options.add_argument('--headless')
        # options.add_argument('window-size=1200x600')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')

        dynamodb = boto3.resource(
            'dynamodb',
            aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
            region_name='ap-northeast-2',
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

        if len(page_href_list) <= 0:
            self.save_href_list()
            return

        self.crawl_page_content(page_href_list)

    def crawling_page_list(self):
        """
        크롤링 대상 page list 만들기
        :return: 크롤링 대상 page list의 링크
        """
        print("gogo")
        prefix_url = 'https://career.programmers.co.kr/job?page='
        page_idx = 0
        cnt = 0
        break_flag = True

        href_list = []
        print("start crawling")
        while break_flag:
            page_idx += 1
            url = prefix_url + str(page_idx)

            browser = webdriver.Chrome(options=self.options)
            browser.get(url)

            titles = browser.find_elements(By.CLASS_NAME, "list-position-item")  # class로 가져오기

            for i, tit in enumerate(titles):

                body = tit.find_element(By.TAG_NAME, "a")  # 태그로 가져오기
                # print(body.get_attribute("href"))
                href = body.get_attribute("href")
                if href in self.check_href_list:
                    break_flag = False
                    print(self.next_href_list)
                    break

                if cnt < self.check_list_size:
                    cnt += 1
                    self.next_href_list.append(href)

                href_list.append(href)

            if cnt < self.check_list_size:
                self.next_href_list.extend(self.check_href_list)

            browser.quit()

        return href_list

    def crawl_page_content(self, page_href_list: list):
        #result = pd.DataFrame(index=range(0, 0), columns=['title', 'company', 'task', 'deadline', 'contract_type', 'career', 'location'])

        content_name_dict = dict()
        content_name_dict["경력"] = "career"
        content_name_dict["고용 형태"] = "work_condition"
        content_name_dict["근무 위치"] = "location"
        content_name_dict["지원 마감"] = "due"
        content_name_dict["연봉"] = "salary"
        content_name_dict["직무"] = "task"

        today = datetime.now().strftime('%Y-%m-%d')
        json_data = []

        for i, href in enumerate(page_href_list):
            #print(href)
            browser = webdriver.Chrome(options=self.options) #service=self.service,
            browser.get(href)
            title = browser.find_element(By.CLASS_NAME, "KWImVsDeFt2E93NXqAqt").find_element(By.TAG_NAME, "h2").text

            company = browser.find_element(By.CLASS_NAME, "e_ow99Z6WyqEsY3oG3gk").text

            body = browser.find_element(By.CLASS_NAME, "oSd94NeynGy8qiuPFFgg")
            contents = body.text.split('\n')
            try:
                content_body = browser.find_elements(By.CLASS_NAME, "yO7TZRtCO7sznD0Csuw_")[1].text
            except:
                content_body = ""

            try:
                stacks = browser.find_element(By.CLASS_NAME, "section-stacks")

                stack_list = stacks.text.split('\n')[1:]
            except:
                stack_list = []

            content_list = []
            idx = 0
            while len(contents) > idx:
                try:
                    content_list.append([content_name_dict[contents[idx]], contents[idx + 1]])
                except:
                    break
                idx = idx + 2


            data = [['title', title],
                    ['company', company],
                    ['crawle_day', today],
                    ['link', href],
                    ['stacks', stack_list],
                    ['body', content_body]
                    ]

            data.extend(content_list)
            #print(data)
            to_dict = dict(data)

            try:
                string = "20" + to_dict['due'].replace(" ", "")
                match = re.findall(r'\d{4}년\d{2}월\d{2}일', string)[0]
                date_temp = datetime.strptime(match, '%Y년%m월%d일')
                to_dict['due'] = date_temp.strftime("%Y-%m-%d")
            except:
                to_dict['due'] = dt(2999, 12, 31).strftime('%Y-%m-%d')

            #경력 제한 후처리
            career_parse_list = self.__parse_career(to_dict['career'])

            if len(career_parse_list) == 2:
                to_dict['career_start'] = career_parse_list[0]
                to_dict['career_end'] = career_parse_list[1]

            temp = json.dumps(to_dict, ensure_ascii=False)
            # json.dump(json.dumps(dict(data), ensure_ascii=False), f, ensure_ascii=False, indent=4)
            json_data.append(json.loads(temp))

            browser.close()
            # if i > 1:
            #     break
            #time.sleep(1)  # 2초 간격 크롤링

        with open('data.ndjson', 'w', encoding='UTF-8-sig') as f:
            ndjson.dump(json_data, f, ensure_ascii=False)

        self.save_href_list()

    def __parse_career(self, career_str: str):
        career_parse_list = []

        if '~' in career_str:
            try:
                career_temp_list = career_str.split('~')
                career_start = re.sub(r'[^0-9]', '', career_temp_list[0])
                career_parse_list.append(int(career_start))

                career_end = re.sub(r'[^0-9]', '', career_temp_list[1])
                career_parse_list.append(int(career_end))
            except:
                career_parse_list = []

        return career_parse_list




#ProgrammersCrawler().crawling_start()
