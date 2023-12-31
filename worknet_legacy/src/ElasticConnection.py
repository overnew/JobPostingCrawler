from elasticsearch import Elasticsearch, helpers
import csv


def uploadCsvToCloud():
  # Password for the 'elastic' user generated by Elasticsearch
  ELASTIC_PASSWORD = ""
  # Found in the 'Manage Deployment' page
  CLOUD_ID = ""
  # Create the client instance
  client = Elasticsearch(
    cloud_id=CLOUD_ID,
    basic_auth=("elastic", ELASTIC_PASSWORD)
  )
  # Successful response!
  info = client.info()
  print(info)
  mapping = {
    "mappings": {
      "properties": {
        "post_id": {
          "type": "keyword"
        },
        "경력조건": {
          "type": "text"
        },
        "고용형태": {
          "type": "text"
        },
        "공고링크": {
          "type": "keyword"
        },
        "공고제목": {
          "type": "text"
        },
        "관련직종": {
          "type": "text"
        },
        "근무시간": {
          "type": "text"
        },
        "근무시간/형태": {
          "type": "text"
        },
        "근무예정지": {
          "type": "text"
        },
        "근무형태": {
          "type": "keyword"
        },
        "급여조건": {
          "type": "keyword"
        },
        "모집마감일": {
          "type": "keyword"
        },
        "모집인원": {
          "type": "long"
        },
        "모집직종": {
          "type": "keyword"
        },
        "모집집종": {
          "type": "keyword"
        },
        "병역대체 복무자채용": {
          "type": "text"
        },
        "복리후생": {
          "type": "text"
        },
        "사회보험": {
          "type": "text"
        },
        "외국어능력": {
          "type": "keyword"
        },
        "임금조건": {
          "type": "text"
        },
        "자격면허": {
          "type": "text"
        },
        "장애인 복지시설": {
          "type": "keyword"
        },
        "장애인 채용인원": {
          "type": "keyword"
        },
        "전공": {
          "type": "text"
        },
        "전형방법": {
          "type": "text"
        },
        "접수마감일": {
          "type": "date",
          "format": "iso8601"
        },
        "접수방법": {
          "type": "text"
        },
        "접수시작일": {
          "type": "keyword"
        },
        "제출서류 준비물": {
          "type": "text"
        },
        "직무내용": {
          "type": "text"
        },
        "직종키워드": {
          "type": "text"
        },
        "채용공고 등록일시": {
          "type": "keyword"
        },
        "추가조건": {
          "type": "text"
        },
        "퇴직급여": {
          "type": "keyword"
        },
        "학력": {
          "type": "keyword"
        },
        "회사명": {
          "type": "keyword"
        }
      }
    }
  }
  # es = Elasticsearch(["192.168.192.168"], http_auth=('elastic', ELASTIC_PASSWORD))#, scheme="http", port=9200)
  idx_name = "connection_test"
  pipeline_name = "bulk_api_pipeTest"
  # result = client.indices.create(index=idx_name, body = mapping, ignore=400)
  # result = client.index(index=idx_name, id=img_doc, body=data)
  # print(res)
  with open('crawling_data_proc2.csv', 'rt', encoding='UTF8') as f:
    # field_names = ['post_id',	'회사명',	'공고제목',	'공고링크',	'접수시작일',	'모집인원',	'근무시간/형태',	'접수마감일',	'접수방법',	'장애인', '채용인원',	'복리후생',	'외국어능력',	'퇴직급여',	'사회보험',	'병역대체', '복무자채용',	'근무형태',	'전공',	'직종키워드',
    #               '채용공고 등록일시',	'모집마감일',	'모집집종',	'추가조건',	'장애인 복지시설',	'모집직종',	'전형방법',	'자격면허',	'근무시간',	'제출서류 준비물',	'직무내용',	'학력',	'관련직종',	'고용형태',	'급여조건',	'경력조건',	'근무예정지',	'임금조건']
    # reader = csv.DictReader(f,fieldnames=field_names, delimiter='\t')
    reader = csv.DictReader(f)
    helpers.bulk(client, reader, index=idx_name)  # , document = mapping)#,pipeline=pipeline_name)


