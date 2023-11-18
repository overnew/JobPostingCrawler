import boto3
import os

class SnsWrapper:
    """Encapsulates Amazon SNS topic and subscription functions."""
    def __init__(self, sns_resource):
        """
        :param sns_resource: A Boto3 Amazon SNS resource.
        """
        self.sns_resource = sns_resource

    def select_topic(self):
        """
        선택할 topic을 list중에서 골라내 반환.

        :return: selected topics.
        """
        try:
            topics_iter = self.sns_resource.topics.all()
            for i, topic in enumerate(topics_iter):
                if "TATTOO_Crawling_check" in topic.arn:
                    print(topic)
                    return topic

        except:
            print("SNS topic exception ocurr!!")

    @staticmethod
    def publish_message(topic, message, attributes):
        try:
            att_dict = {}
            for key, value in attributes.items():
                if isinstance(value, str):
                    att_dict[key] = {'DataType': 'String', 'StringValue': value}
                elif isinstance(value, bytes):
                    att_dict[key] = {'DataType': 'Binary', 'BinaryValue': value}
            response = topic.publish(Message=message, MessageAttributes=att_dict)
            message_id = response['MessageId']

        except:
            print("Couldn't publish message to topic %s.", topic.arn)

    @staticmethod
    def publish_crawling_message(message: str):
        key = 'crawling'
        value = 'done'
        resource = boto3.resource(
            'sns',
            aws_access_key_id= os.environ['AWS_SNS_USER_ACCESS_KEY_ID'],
            aws_secret_access_key= os.environ['AWS_SNS_USER_SECRET_ACCESS_KEY'],
            region_name='ap-northeast-2',
        )
        sns_wrapper = SnsWrapper(resource)
        topic = sns_wrapper.select_topic()
        sns_wrapper.publish_message(topic, message, {key: value})

