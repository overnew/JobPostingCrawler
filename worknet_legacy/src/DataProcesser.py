import pandas as pd
import numpy as np
import re
from datetime import datetime
from datetime import date as dt

class Processer:

    ##더 좋은 데이터가 target1으로
    def mergeField(self, data , target1, target2):
        """
        target1 data가 없다면 target2를 넣어준 후, target2를 삭제
        :param data: pd.DataFrame
        :param target1: column 명
        :param target2: column 명
        :return:
        """
        for i, row in data.iterrows():
            tar1 = row[target1]
            tar2 = row[target2]
            if tar1 == np.NaN:
                data.loc[i, target1] = tar2

        #data = data.drop(columns=[target2])

    def setStringToNumber(self, data, columnName):
        for i, row in data.iterrows():
            str = row[columnName]
            if str == np.NaN:
                continue

            data.loc[i, columnName] = self.getNumberInString(str)


    def getNumberInString(self, string):
        """
        문자열에서 1회이상 반복되는 첫번쨰 숫자 추출
        :param string:
        :return:
        """
        numbers = re.findall(r'\d+', string)
        if len(numbers) == 0:
            return 0
        return numbers[0]

    def procEndDate(self,data, col):
        for i, row in data.iterrows():
            date = None
            string = row[col].replace(" ","")
            matches = re.findall(r'\d{4}년\d{2}월\d{2}일', string)
            if len(matches) >=2:
                date1 = datetime.strptime(matches[0], '%Y년%m월%d일').date()
                date2 = datetime.strptime(matches[1], '%Y년%m월%d일').date()
                date = date2

                if date1 > date2:
                    date = date1
            elif len(matches) == 1:
                date = datetime.strptime(matches[0], '%Y년%m월%d일').date()

            if date != None:
                data.loc[i, col] = date
                continue

            matches = re.findall(r'\d{4}.\d{2}.\d{2}', string)
            if len(matches) == 1:
                date = datetime.strptime(matches[0], '%Y.%m.%d').date()
                data.loc[i, col] = date
            else:
                data.loc[i, col] = dt(2999, 12, 31)

    def getOrderNames(self, list2):
        names = ['post_id', '회사명', '공고제목', '공고링크']
        complement = list(set(list2) - set(names))
        return names + complement

    def proc(self):
        data = pd.read_csv("crawling_data.csv")

        #의미x 컬럼 제거
        keepColumns = [
          "post_id","공고링크","공고제목","회사명",
          "경력조건", "고용형태","관련직종",
          "근무시간", "근무시간/형태","근무예정지",
          "근무형태", "급여조건",
          "모집마감일", "모집인원","모집직종","모집집종",
          "병역대체 복무자채용",
          "기타 복리후생", "복리후생",
          "사회보험",
          "외국어능력",
          "임금조건",
          "자격면허",
          "장애인 복지시설", "장애인 채용인원",
          "전공", "전형방법", "접수방법",
          "접수시작일", "접수마감일",
          "제출서류 준비물",
          "직무내용", "직종키워드",
          "채용공고 등록일시",
          "추가조건",
          "퇴직금지급방법", "퇴직급여",
          "학력", "학력조건"
        ]
        drop_targets = list(set(data.columns) - set(keepColumns))
        data = data.drop(columns= drop_targets)

        #중복 내용 컬럼 제거
        try:
            self.mergeField(data,"복리후생", "기타 복리후생")
            data =data.drop("기타 복리후생", axis=1)
        except:
            print("no merge1")

        try:
            self.mergeField(data,"학력", "학력조건")
            data =data.drop("학력조건", axis=1)
        except:
            print("no merge2")

        try:
            self.mergeField(data,"퇴직급여", "퇴직금지급방법")
            data = data.drop("퇴직금지급방법", axis=1)
        except:
            print("no merge3")

        #모집인원 숫자만 때오기
        self.setStringToNumber(data,'모집인원')

        #채용 마감일 처리하기
        self.procEndDate(data, "접수마감일")


        #그래서 처리한 데이터를 글라우드로 보내는거 물어보기

        df = pd.DataFrame(data)
        df = df[self.getOrderNames(df.columns)]
        df.to_csv("crawling_data_proc2.csv", index=False, encoding="utf-8-sig")
