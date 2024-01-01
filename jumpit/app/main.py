from ElasticConnection import uploadCsvToCloud
from JumpItCrawler import JumpItCrawler
from SlackNotifiaction import SnsWrapper

try:
    JumpItCrawler().crawl_start()
    #uploadCsvToCloud()
except: #에러 시에만 알림
    SnsWrapper.publish_crawling_message("점핏 크롤링 에러!")