from elasticsearch import Elasticsearch, helpers
import csv
import os

def uploadCsvToCloud(file_name: str):
  ELASTIC_PASSWORD = os.environ['ELASTIC_CLOUD_PASSWORD']

  # Found in the 'Manage Deployment' page
  CLOUD_ID = os.environ['ELASTIC_CLOUD_ID']
  # Create the client instance
  client = Elasticsearch(
    cloud_id=CLOUD_ID,
    basic_auth=("elastic", ELASTIC_PASSWORD)
  )


  idx_name = "worknet_fix"

  with open(file_name, 'rt', encoding='UTF8') as f:
    reader = csv.DictReader(f)
    helpers.bulk(client, reader, index=idx_name)  # , document = mapping)#,pipeline=pipeline_name)

  print("cloud upload complete!")

