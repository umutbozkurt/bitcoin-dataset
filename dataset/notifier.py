import boto3

TOPIC = 'arn:aws:sns:us-east-1:386328421765:btc-collector'

client = boto3.client('sns')


def notify(subject, message):
    client.publish(
        TopicArn=TOPIC,
        Subject=subject,
        Message=message,
        MessageStructure='string'
    )
