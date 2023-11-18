from elasticsearch import Elasticsearch, helpers
import os

def uploadCsvToCloud():
  # Password for the 'elastic' user generated by Elasticsearch
  ELASTIC_PASSWORD = os.environ['ELASTIC_CLOUD_PASSWORD']

  # Found in the 'Manage Deployment' page
  CLOUD_ID = os.environ['ELASTIC_CLOUD_ID']

  # Create the client instance
  client = Elasticsearch(
    cloud_id=CLOUD_ID,
    basic_auth=("elastic", ELASTIC_PASSWORD)
  )

  #idx_name = "p1t-programmers_final_ver2"
  idx_name = "jumpit_crawl_ver1"
  #pipeline_name = "programmers_change_career_format"

  with open('data.ndjson', 'rt', encoding='UTF-8-sig') as f:
    helpers.bulk(client, f.readlines(), index=idx_name)#, pipeline=pipeline_name)
  print("upload complete!")
#uploadCsvToCloud()

