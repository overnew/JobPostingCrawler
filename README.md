# JobPostingCrawler

<strong>취업 공고 사이트에서 크롤링한 데이터를 Elastic Cloud로 전송하는 코드 입니다.
 - 크롤링 코드는 도커로 빌드 되어 AWS ECR에 전송됩니다.

<br>

 - 크롤링 완료시 AWS SNS Topic에 완료 메시지를 Publish 합니다.
 
<br>

- [검색 기능 Slack Bot GitHub 보러가기](https://github.com/overnew/JobPostingSearchingBot)
<br><br><br><br>


## 현재 대상 취업 공고 사이트
 - 프로그래머스
 - 워크넷

<br><br><br>

## 서비스 전체 아키텍처
<br><br>

### 크롤링과 Slack Bot

<img width="80%" src="https://github.com/overnew/JobPostingCrawler/assets/43613584/ce609c86-8cff-4ca0-8d4e-b9a2e5b70b17"/>

<br><br>

### 구독 기능

<img width="80%" src="https://github.com/overnew/JobPostingSearchingBot/assets/43613584/ea06fbc7-cb29-4aff-8a60-687b592372f1" />


<br><br><br>

