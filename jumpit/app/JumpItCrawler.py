from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

from datetime import datetime
from datetime import date as dt
from DynamoDBConnector import DynamoDBConnector

import time
import re
import json
import ndjson


class JumpItCrawler:

    def __init__(self):
        self.options = webdriver.ChromeOptions()
        self.options.add_argument('--headless')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')

        self.db_connector = DynamoDBConnector()
        self.crawled_list = self.db_connector.get_check_list()
        self.next_check_list = []
        self.check_list_size = 10

        self.json_data = []

    def crawl_start(self):
        crawling_target_list = self.__make_crawl_list()

        if len(crawling_target_list) <= 0:
            return

        print("get " + str(len(crawling_target_list)))
        print(crawling_target_list)

        self.__crawl_list(crawling_target_list)

        # njson으로 저장
        with open('data.ndjson', 'w', encoding='UTF-8-sig') as f:
            ndjson.dump(self.json_data, f, ensure_ascii=False)

        self.__make_next_check_list(crawling_target_list)
        self.db_connector.save_check_list(self.next_check_list)

    def __make_next_check_list(self, crawling_target_list: list):
        if len(crawling_target_list) < self.check_list_size:
            self.next_check_list.extend(self.crawled_list)

        self.next_check_list.extend(crawling_target_list)
        return

    def __make_crawl_list(self):
        crawling_target_list = []

        url = "https://www.jumpit.co.kr/positions?sort=reg_dt"
        browser = webdriver.Chrome(options=self.options)
        browser.get(url)

        actions = browser.find_element(By.CSS_SELECTOR, 'body')

        break_flag = False
        start_idx = 0

        print("start crawling")
        while not break_flag:
            postings = browser.find_elements(By.XPATH, "/html/body/main/div/div[1]/section/div")

            for idx in range(start_idx, len(postings)-1):
                href = postings[idx].find_element(By.TAG_NAME, "a").get_attribute("href")

                if href in self.crawled_list:
                    break_flag = True
                    break

                print(href)
                crawling_target_list.append(href)

            start_idx = len(postings)

            actions.send_keys(Keys.END)
            time.sleep(3)

        browser.quit()
        return crawling_target_list

    def __crawl_list(self, crawling_target_list: list):
        for i, href in enumerate(crawling_target_list):
            self.__get_detail_by_page(href)

    def __get_detail_by_page(self, href: str):
        browser = webdriver.Chrome(options=self.options)
        browser.get(href)
        time.sleep(1)

        # 크롤링 내용
        title = browser.find_element(By.XPATH, "/html/body/main/div/div[2]/div/section[1]/h1").text

        company = browser.find_element(By.XPATH, "/html/body/main/div/div[2]/div/section[1]/div/a").text

        post_body = browser.find_element(By.XPATH, "/html/body/main/div/div[2]/div/section[2]").text
        career = browser.find_element(By.XPATH, "/html/body/main/div/div[2]/div/section[3]/dl[1]/dd").text

        location = browser.find_element(By.XPATH,
                                        "/html/body/main/div/div[2]/div/section[3]/dl[4]/dd/ul/li").text
        location = location.replace("\n지도보기·주소복사", "")

        due = browser.find_element(By.XPATH, "/html/body/main/div/div[2]/div/section[3]/dl[3]/dd").text
        salary = "추후 협의"
        crawle_day = datetime.now().strftime('%Y-%m-%d')

        # 요구 기술 스택
        stacks = browser.find_elements(By.XPATH,"/html/body/main/div/div[2]/div/section[2]/dl[1]/dd/pre/div")

        stack_list = []
        try:
            for i, stack in enumerate(stacks):
                stack_list.append(stack.text)
        except:
            stack_list = []

        # 마감일 처리
        try:
            re.findall(r'\d{4}-\d{2}-\d{2}', due)[0]
        except:
            due = dt(2999, 12, 31).strftime('%Y-%m-%d')

        # dict으로 저장
        data = [['title', title],
                ['company', company],
                ['crawle_day', crawle_day],
                ['link', href],
                ['body', post_body],
                ['location', location],
                ['salary', salary],
                ['due', due],
                ['stacks', stack_list]]

        to_dict = dict(data)

        # 경력 제한 후처리
        career_parse_list = []
        try:
            career_parse_list = self.__parse_career(career)
        except:
            career_parse_list = [0, 50]  # 경력 무관

        if len(career_parse_list) == 2:
            to_dict['career_start'] = career_parse_list[0]
            to_dict['career_end'] = career_parse_list[1]

        print(to_dict)
        temp = json.dumps(to_dict, ensure_ascii=False)
        self.json_data.append(json.loads(temp))
        browser.close()

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
                career_parse_list = [0, 50]

        return career_parse_list

