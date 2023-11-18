from ElasticConnection import uploadCsvToCloud
from programersCrawler import ProgrammersCrawler
from SlackNotifiaction import SnsWrapper

try:
    ProgrammersCrawler().crawling_start()
    uploadCsvToCloud()
except:
    SnsWrapper.publish_crawling_message("프로그래머스 크롤링 에러!")
