import requests
from bs4 import BeautifulSoup
from bs4.element import Tag
import time
import pandas as pd
import re

from datetime import datetime
from datetime import date as dt

from ElasticConnection import uploadCsvToCloud


def write_data_info(file_route: str, log_route: str) -> None:
    res = open(log_route, 'w', encoding='utf-8')

    data = pd.read_csv(file_route, header=0, encoding='utf-8')
    #print(data.info())
    res.write(f'-----\n각 column별 NaN 값 개수\n-----\n')
    res.write(f'{data.isnull().sum()}\n\n')

    res.write(f'-----\n각 column별 유니크 값 개수\n-----\n')
    for col in data.columns:
        res.write(f'{col}: {data[col].nunique()}')
        if data[col].nunique() < 10:
            res.write(f', {data[col].unique()}')
        res.write('\n')

    res.close()
    return


def make_url(base_url: str, params: dict) -> str:
    url = base_url
    for key in params.keys():
        url += (key + '=' + str(params[key]) + '&')
    return url[:-1]


def template_empty(cols: list) -> pd.DataFrame:
    res = dict()
    for col in cols:
        res[col] = []
    return pd.DataFrame(res)


def template_one_row(cols: list, values: list) -> pd.DataFrame:
    res = dict()
    for col, value in zip(cols, values):
        res[col] = [value]
    return pd.DataFrame(res)


def safe_text(content: Tag) -> str:
    if content is not None:
        text = content.text.strip()
        return re.sub(r"\s+", " ", text)
    else:
        return None


def scrap_post(link: str) -> list:  # ['task', 'work_time', 'due']
    post = requests.get(link)
    post = BeautifulSoup(post.content, 'html.parser')

    # 직무
    task_selector = ['#contents > section > div > div.careers-area > div:nth-child(4) > table > tbody > tr > td',
                     '#contents > div.careers-area > div:nth-child(4) > table > tbody > tr > td:nth-child(1)']
    task = ''
    for selector in task_selector:
        task = safe_text(post.select_one(selector))
        if task is not None:
            break
    if task is None or task == '':
        task = 'no_content'

    # 근무 시간
    work_time_selector = ['#contents > section > div > div.careers-area > div.careers-new > div.border > div.left > div:nth-child(3) > div:nth-child(1) > div > ul > li:nth-child(2) > span',
                          '#contents > div.careers-area > div:nth-child(12) > table > tbody > tr > td:nth-child(2)',
                          '#contents > div.careers-area > div.careers-table.v1.center.mt20 > table > tbody > tr > td:nth-child(2)']
    work_time = ''
    for selector in work_time_selector:
        work_time = safe_text(post.select_one(selector))
        if work_time is not None:
            break
    if work_time is None or work_time == '':
        work_time = 'no_content'

    # 마감일
    due_selector = ['#contents > section > div > div.careers-area > div:nth-child(11) > table > tbody > tr > td:nth-child(1)',
                    '#contents > div.careers-area > div:nth-child(7) > table > tbody > tr > td:nth-child(1)',
                    '#contents > div.careers-area > div:nth-child(6) > table > tbody > tr > td:nth-child(1)']
    due = ''
    for selector in due_selector:
        due = safe_text(post.select_one(selector))
        if due is not None:
            break
    if due is None or due == '':
        due = 'no_content'

    # 근무 조건: 정규/비정규, 주n일
    work_condition_selector = '#contents > div.careers-area > div.careers-table.v1.center.mt20 > table > tbody > tr > td:nth-child(2)'
    work_condition = safe_text(post.select_one(work_condition_selector))
    if str(work_condition).find('주') < 0:
        work_condition = None
    if work_condition is None or work_condition == '':
        work_condition = 'no_content'

    return [task, work_time, due, work_condition]


def proc_end_date(due_data:str):
    date = None

    try:
        matches = re.findall(r'\d{4}년.\d{2}월.\d{2}일', due_data)
        if len(matches) >= 2:
            date1 = datetime.strptime(matches[0], '%Y년 %m월 %d일').date()
            date2 = datetime.strptime(matches[1], '%Y년 %m월 %d일').date()
            date = date2

            if date1 > date2:
                date = date1
        else:
            date = dt(2999, 12, 31)
    except:
        date = dt(2999, 12, 31)

    return date


base_url = 'https://www.work.go.kr/empInfo/empInfoSrch/list/dtlEmpSrchList.do?'
params = {
    'careerTo': '',
    'keywordJobCd': '',
    'occupation': '',
    'templateInfo': '',
    'shsyWorkSecd': '',
    'rot2WorkYn': '',
    'payGbn': '',
    'resultCnt': 50,  # 결과 개수
    'keywordJobCont': '',
    'cert': '',
    'cloDateStdt': '',  # 공고 마감일 시작 날짜 '20230803'
    'moreCon': 'more',
    'minPay': '',  # 최소 희망 임금(만) '0'
    'codeDepth2Info': '11000',
    'isChkLocCall': '',
    'sortFieldInfo': 'DATE',  # 정렬 기준
    'major': '',
    'resrDutyExcYn': '',
    'eodwYn': '',
    'sortField': 'DATE',  # 정렬 기준 필드
    'staArea': '',
    'sortOrderBy': 'DESC',  # 정렬 방향
    'keyword': '',  # 검색 키워드 통합(검색, 제외)
    'termSearchGbn': 'all',
    'carrEssYns': '',
    'benefitSrchAndOr': 'O',
    'disableEmpHopeGbn': '',
    'webIsOut': '',
    'actServExcYn': '',
    'maxPay': '',  # 최대 희망 임금(만), 최대 6자리 '100000'
    'keywordStaAreaNm': '',
    'emailApplyYn': '',
    'listCookieInfo': 'DTL',
    'pageCode': '',
    'codeDepth1Info': '11000',
    'keywordEtcYn': '',
    'publDutyExcYn': '',
    'keywordJobCdSeqNo': '',
    'exJobsCd': '',
    'templateDepthNmInfo': '',
    'computerPreferential': '',
    'regDateStdt': '',  # 등록일 시작 날짜 '20230704'
    'employGbn': '',
    'empTpGbcd': '',
    'region': '',  # 지역(지역코드) '11000'
    'infaYn': '',
    'resultCntInfo': 50,  # 결과 개수
    'siteClcd': 'all',  # 정보제공처 'GOJ'
    'cloDateEndt': '',  # 마감일 종료 일자 '20230809'
    'sortOrderByInfo': 'DESC',  # 정렬 방향
    'currntPageNo': 1,  # 현재 페이지
    'indArea': '',
    'careerTypes': '',
    'searchOn': '',
    'tlmgYn': '',
    'subEmpHopeYn': '',
    'academicGbn': '',  # 학력 '05'
    'templateDepthNoInfo': '',
    'foriegn': '',
    'mealOfferClcd': '',  # 식사제공 '2'
    'station': '',
    'moerButtonYn': 'Y',
    'holidayGbn': '',  # 근무형태(주n일) '4'
    'srcKeyword': '',  # 검색 키워드
    'enterPriseGbn': '',
    'academicGbnoEdu': '',
    'cloTermSearchGbn': '',
    'keywordWantedTitle': 'N',
    'stationNm': '',  # 역세권
    'benefitGbn': '',  # 기타 복리후생
    'keywordFlag': '',
    'notSrcKeyword': '',  # 제외 키워드
    'essCertChk': '',
    'isEmptyHeader': '',
    'depth2SelCode': '',
    '_csrf': '72229076-7aac-4af3-b833-9a08ff488d50',
    'keywordBusiNm': '',
    'preferentialGbn': '',
    'rot3WorkYn': '',
    'pfMatterPreferential': '',
    'regDateEndt': '',  # 공고 등록일 종료 일자 '20230803'
    'staAreaLineInfo1': '',  # 지하철 지역 코드 '11000'
    'staAreaLineInfo2': '',  # 지하철 노선번호 '1'
    'pageIndex': 1,  # 페이지 인덱스
    'termContractMmcnt': '',
    'careerFrom': '',
    'laborHrShortYn': '#viewSPL'
    }

params['regDateStdt'] = time.strftime('%Y%m%d')
params['regDateEndt'] = time.strftime('%Y%m%d')
#print(time.strftime('%Y%m%d'))

# 검색 조건에 따른 결과 개수 확인
params['resultCnt'] = 10
params['resultCntInfo'] = 10

# url 생성
url = make_url(base_url, params)

# print(url, '\n')
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')

cnt = safe_text(soup.select_one(f'#mForm > div.board-list-count.mt50 > p > strong')).replace(',', '')
if cnt is not None:
    cnt = int(cnt)
#print(cnt)



# 저장할 리스트 생성
cols = [
    'post_id',
    'company',
    'title',
    'link',
    'career',
    'education',
    'location',
    'salary_type',
    'salary',
    'work_condition',
    'task',
    'work_time',
    'due',
    'crawle_day'
    ]

#print(cols)

res = open('crawl-worknet.log', 'w', encoding='utf-8')

# 페이지당 검색 개수 변경: 10, 30, 50
params['resultCnt'] = 50
params['resultCntInfo'] = 50
crawle_day = time.strftime('%Y-%m-%d')
data = template_empty(cols)

for page in range(1, 20):# cnt // params['resultCnt'] + 1):
    params['currntPageNo'] = page
    params['pageIndex'] = page
    # url 생성
    url = make_url(base_url, params)

    # print(url, '\n')
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    selector_base = 'div.table-wrap > table.board-list > tbody > '

    buffer = template_empty(cols)

    for row in range(1, params['resultCnt'] + 1):
        res.write(f'page {page}, row {row}\n')
        #print(f'page {page}, row {row}')

        # 회사명
        company_selector = selector_base + f'tr#list{row} > td:nth-child(2) > a.cp_name'
        company = safe_text(soup.select_one(company_selector))
        res.write(f'company: {company}\n')

        # 공고명
        title_selector = selector_base + f'tr#list{row} > td:nth-child(3) > div > div > a'
        title_tag = soup.select_one(title_selector)
        title = safe_text(title_tag)
        res.write(f'title: {title}\n')

        # 공고 링크
        link = ''
        try:
            link = 'https://www.work.go.kr' + title_tag['href']
            if link.find('rtnUrl') != -1:
                link = link[:link.find('rtnUrl') - 1]
        except:
            link = None
        res.write(f'link: {link}\n')
        #print(f'link: {link}')

        # 경력
        career_selector = selector_base + f'tr#list{row} > td:nth-child(3) > div > p:nth-child(3) > em:nth-child(1)'
        career = safe_text(soup.select_one(career_selector))
        res.write(f'career: {career}\n')

        # 학력
        education_selector = selector_base + f'tr#list{row} > td:nth-child(3) > div > p:nth-child(3) > em:nth-child(2)'
        education = safe_text(soup.select_one(education_selector))
        res.write(f'education: {education}\n')

        # 지역
        location_selector = selector_base + f'tr#list{row} > td:nth-child(3) > div > p:nth-child(3) > em:nth-child(3)'
        location = safe_text(soup.select_one(location_selector))
        res.write(f'location: {location}\n')

        # 급여 유형
        salary_type_selector = selector_base + f'tr#list{row} > td:nth-child(4) > div > p:nth-child(1) > strong'
        salary_type = safe_text(soup.select_one(salary_type_selector))
        res.write(f'salary type: {salary_type}\n')

        # 급여 금액
        salary_selector = selector_base + f'tr#list{row} > td:nth-child(4) > div > p:nth-child(1)'
        salary = safe_text(soup.select_one(salary_selector)).replace(salary_type, "").strip()
        if salary_type == '회사내규에 따름':
            salary = "회사내규에 따름"
        #     salary = -1
        # else:
        #     try:
        #         salary = salary.replace(",", "")
        #         numbers = re.findall(r'\d+', salary)
        #         salary = numbers[0]
        #     except:
        #         salary = -1

        res.write(f'salary: {salary}\n')

        # 근무 조건: 정규/비정규, 주n일
        work_condition_selector = selector_base + f'tr#list{row} > td:nth-child(4) > div > p:nth-child(3)'
        work_condition = safe_text(soup.select_one(work_condition_selector))
        res.write(f'work condition: {work_condition}\n')

        # 담당업무: 여기서는 수집 불가
        task = ''

        # 일일 근무 시간
        work_time = ''

        # 채용 마감일
        due = ''

        # 공고 id
        post_id = ''

        if link is not None:
            id_idx = link.find('wantedAuthNo=') + len('wantedAuthNo=')
            post_id = link[id_idx:id_idx + link[id_idx:].find('&')]
            this_post = scrap_post(link)  # ['task', 'work_time', 'due', 'work_condition']
            task = this_post[0]
            work_time = this_post[1]
            due = proc_end_date(this_post[2])
            if work_condition is None or len(str(work_condition)) < 1:
                work_condition = this_post[3]

        if due == "":
            due = dt(2999, 12, 31)

        res.write(f'task: {task}\n')
        res.write(f'work time: {work_time}\n')
        res.write(f'due: {due}\n')

        temp = template_one_row(cols, [
            post_id,
            company,
            title,
            link,
            career,
            education,
            location,
            salary_type,
            salary,
            work_condition,
            task,
            work_time,
            due,
            crawle_day
        ])

        #print(temp.T)
        buffer = pd.concat([buffer, temp], ignore_index=True)

        res.write('-----\n')
    # end of for i in range(1, params['resultCnt'] + 1):
    data = pd.concat([data, buffer], ignore_index=True)
# end of for i in range(1, cnt // params['resultCnt'] + 1):

#print(data.info())

file_name = 'crawl_worknet1.csv'
data.to_csv(file_name, encoding='utf-8', index=False)

res.close()

uploadCsvToCloud(file_name)