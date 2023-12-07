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

    # idx_name = "p1t-worknet_final_ver2"
    idx_name = "worknet_crawle_test2"
    union_idx_name = "job_post_union_ver1"
    pipeline_name = "TATTOO_index_integrate_worknet"

    with open(file_name, 'rt', encoding='UTF8') as f:
        reader = csv.DictReader(f)
        helpers.bulk(client, reader, index=idx_name)

        # #통합 인덱스에 업로드
        helpers.bulk(client, f.readlines(), index=union_idx_name, pipeline=pipeline_name)


    print("cloud upload complete!")
