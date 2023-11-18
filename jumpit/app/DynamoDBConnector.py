import boto3
import os


class DynamoDBConnector:

    def __init__(self):
        dynamodb = boto3.resource(
            'dynamodb',
            aws_access_key_id= os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key= os.environ['AWS_SECRET_ACCESS_KEY'],
            region_name='ap-northeast-2',
        )

        table_name = '001_jumpit_crawler_metadata'  # 테이블 이름
        self.table = dynamodb.Table(table_name)
        self.check_href_list = []

    def get_check_list(self):
        temp = self.table.scan()['Items']
        for i, row in enumerate(temp):
            self.check_href_list.append(row['store_id'])

        return self.check_href_list

    def save_check_list(self, next_href_list: list):
        for i, href in enumerate(self.check_href_list):
            self.table.delete_item(
                Key={'store_id': href}
            )

        for i, href in enumerate(next_href_list):
            self.table.put_item(
                Item={
                    'store_id': href
                })
