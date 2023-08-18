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

  idx_name = "programmers"

  with open('data.ndjson', 'rt', encoding='UTF-8-sig') as f:
    helpers.bulk(client, f.readlines(), index=idx_name)  # , document = mapping)#,pipeline=pipeline_name)

#uploadCsvToCloud()

