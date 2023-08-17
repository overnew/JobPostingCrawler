from bs4 import BeautifulSoup
import requests
import pandas as pd
import time
from DataProcesser import Processer
from ElasticConnection import uploadCsvToCloud

def makeCrawlingTargetPageList():
    """
    크롤링 대상이되는 페이지를 csv로 저장
    :return:
    """
    result = pd.DataFrame()

    for i in range(1,3):  #1~100번째 페이지 까지
        prefix_url = "https://www.work.go.kr/empInfo/empInfoSrch/list/dtlEmpSrchList.do?careerTo=&keywordJobCd=&occupation=&templateInfo=&shsyWorkSecd=&rot2WorkYn=&payGbn=&resultCnt=10&keywordJobCont=N&cert=&cloDateStdt=&moreCon=&minPay=&codeDepth2Info=11000&isChkLocCall=&sortFieldInfo=DATE&major=&resrDutyExcYn=&eodwYn=&sortField=DATE&staArea=&sortOrderBy=DESC&keyword=&termSearchGbn=&carrEssYns=&benefitSrchAndOr=O&disableEmpHopeGbn=&webIsOut=&actServExcYn=&maxPay=&keywordStaAreaNm=N&emailApplyYn=&listCookieInfo=DTL&pageCode=&codeDepth1Info=11000&keywordEtcYn=&publDutyExcYn=&keywordJobCdSeqNo=&exJobsCd=&templateDepthNmInfo=&computerPreferential=&regDateStdt=&employGbn=&empTpGbcd=1&region=&infaYn=&resultCntInfo=10&siteClcd=all&cloDateEndt=&sortOrderByInfo=DESC&currntPageNo=1&indArea=&careerTypes=&searchOn=&tlmgYn=&subEmpHopeYn=&academicGbn=&templateDepthNoInfo=&foriegn=&mealOfferClcd=&station=&moerButtonYn=&holidayGbn=&srcKeyword=&enterPriseGbn=&academicGbnoEdu=&cloTermSearchGbn=&keywordWantedTitle=N&stationNm=&benefitGbn=&keywordFlag=&notSrcKeyword=&essCertChk=N&isEmptyHeader=&depth2SelCode=&_csrf=792b2f42-ab91-4a6e-940e-1986afe8b6de&keywordBusiNm=N&preferentialGbn=&rot3WorkYn=&pfMatterPreferential=&regDateEndt=&staAreaLineInfo1=11000&staAreaLineInfo2=1&pageIndex="
        postfix_url = "&termContractMmcnt=&careerFrom=&laborHrShortYn=#viewSPL"
        url = prefix_url + str(i) + postfix_url
        response = requests.get(url)
        print(str(i) + "running!")
        html_content = response.text

        ret = makeTodayList(html_content)
        result = pd.concat([result, ret])
        time.sleep(2)   #2초 간격 크롤링

    # target list 저장
    df = pd.DataFrame(result)
    df.to_csv("target_list.csv",index=False, encoding="utf-8-sig")


def makeTodayList(html_content):
    """
    각 최신 공고 페이지에서 10개의 Crawling Target Page를 가져옴
    :param html_content:
    :return:
    """

    result = pd.DataFrame()

    soup = BeautifulSoup(html_content, "lxml")
    for i in range(1, 11):
        try:
            list_id = "list" + str(i)
            art = soup.find("tr", id=list_id)
            ret = readHtml(art)
            result = pd.concat([result,ret])
        except:
            print("except occur")

    return result

def readHtml(art):

    for_title = art.find("td", "a-l va-t")
    name = for_title.find("a").text.strip()

    for_link = art.find("div", "cp-info-in")
    title = for_link.find("a").text.strip()
    link = for_link.find("a").attrs['href']

    post_id = None

    if link is not None:
        prefix = "https://www.work.go.kr/"
        split_list = link.split("&")

        for i, str in enumerate(split_list):
            if "wantedAuthNo=" in str:
                post_id = str.replace("wantedAuthNo=","")
                break
            prefix += str + "&"

        if post_id is not None:
            link = prefix + "wantedAuthNo="+ post_id
        else:
            link = "no_link"
    else:
        link = "no_link"

    data = {'post_id': ["_"+post_id], '회사명': [name], '공고제목': [title], '공고링크': [link]}

    return pd.DataFrame(data)



### 이후는 세부 페이지 크롤링

def getUnionName(lst1, lst2):
    """
    두 list에서 교집합을 반환
    :param lst1:
    :param lst2:
    :return:
    """
    intersection = list(set(lst1) & set(lst2))
    return intersection

def getOrderNames(list2):
    """
    pandas dataFrame의 column 순을 변경
    :param list2:
    :return:
    """
    names = ['post_id','회사명', '공고제목','공고링크']
    complement = list(set(list2) - set(names))
    return names + complement

def allDataMerge():
    """
    만들어둔 target_list에서 링크를 가져와서 세부적인 내용을 크롤링
    :return:
    """
    detail_data = pd.DataFrame()
    data = pd.read_csv("target_list.csv")
    flag = True
    for i, row in data.iterrows():
        try:
            href = row['공고링크']
            if href == "no_link":
                continue
            response = requests.get(href)

            print(str(i) + " running!")

            html_content = response.text
            ret = read_detail(html_content)
        except:
            continue


        ret['post_id'] = row['post_id']
        ret['회사명'] = row['회사명']
        ret['공고제목'] = row['공고제목']
        ret['공고링크'] = row['공고링크']

        ret.set_index('post_id')
        #print(ret)
        if flag:
            detail_data = ret
            flag = False
        else:
            detail_data = pd.merge(detail_data, ret, how = 'outer')

        time.sleep(1)   #1초 간격 크롤링



    df = pd.DataFrame(detail_data)
    df = df[getOrderNames(df.columns)]
    df.to_csv("crawling_data.csv", index=False, encoding="utf-8-sig")

def read_detail(html_content):
    """
    html의 table 테그 대상으로 파싱
    :param html_content:
    :return:
    """

    soup = BeautifulSoup(html_content, "lxml")

    tables = soup.select("table")
    table_df_list = pd.read_html(str(tables))   #pandas의 read_html사용

    result = pd.DataFrame()
    for i, data in enumerate(table_df_list):
        if(data.shape[0] == 1):
            uni = getUnionName(result.columns, data.columns)
            if len(uni) != 0:
                data.drop(uni, axis=1, inplace=True)
            result = pd.concat([result, data], axis=1)

    return result.dropna(axis=1)


# lxml 설치 후 실행
makeCrawlingTargetPageList()
allDataMerge()

Processer().proc()
uploadCsvToCloud()



#https://www.work.go.kr/empInfo/empInfoSrch/detail/empDetailAuthView.do?searchInfoType=P&callPage=detail&wantedAuthNo=DdA3092307282012&rtnTarget=list8&pageIndex=10&rtnUrl=/empInfo/empInfoSrch/list/dtlEmpSrchList.do?len=0&regionArr=[Ljava.lang.String;@6981020e&pageSize=10&firstIndex=1&lastIndex=1&recordCountPerPage=10&siteClcd=all&benefitSrchAndOr=O&areaRegion=11000&areaArc=1&staAreaLineInfo1=11000&staAreaLineInfo2=1&codeDepth1Info=11000&codeDepth2Info=11000&listCookieInfo=DTL&srchJobNum=0&resultCntInfo=10&sortOrderByInfo=DESC&sortFieldInfo=DATE&essCertChk=N&empTpGbcd=1&sortField=DATE&sortOrderBy=DESC&resultCnt=10&&onlyTitleSrchYn=N&keywordWantedTitle=N&keywordBusiNm=N&keywordJobCont=N&keywordStaAreaNm=N
#https://www.work.go.kr/empInfo/empInfoSrch/detail/retrivePriEmpDtlView.do?searchInfoType=CSI&iorgGbcd=CSI&callPage=detail&wantedAuthNo=46242851&rtnTarget=list1&pageIndex=30&rtnUrl=/empInfo/empInfoSrch/list/dtlEmpSrchList.do?len=0&regionArr=[Ljava.lang.String;@4d19fd5a&pageSize=10&firstIndex=1&lastIndex=1&recordCountPerPage=10&siteClcd=all&benefitSrchAndOr=O&areaRegion=11000&areaArc=1&staAreaLineInfo1=11000&staAreaLineInfo2=1&codeDepth1Info=11000&codeDepth2Info=11000&listCookieInfo=DTL&srchJobNum=0&resultCntInfo=10&sortOrderByInfo=DESC&sortFieldInfo=DATE&essCertChk=N&empTpGbcd=1&sortField=DATE&sortOrderBy=DESC&resultCnt=10&&onlyTitleSrchYn=N&keywordWantedTitle=N&keywordBusiNm=N&keywordJobCont=N&keywordStaAreaNm=N
