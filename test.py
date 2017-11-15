import sys
import boto3
import math
import json


def create_message():
    message = {}
    message['Origin'] = 'test'
    message['Value'] = 12
    return message


stream = sys.argv[1]
messages_num = int(sys.argv[2])
client = boto3.client('firehose')
while messages_num > 0:
    current_batch_num = min(messages_num, 500)
    messages = [create_message() for current_batch_num in range(500)]
    records = [{
        'Data': bytes(json.dumps(message) + '\n', 'utf-8')
    } for message in messages]
    response = client.put_record_batch(DeliveryStreamName=stream, Records=records)
    if int(response['FailedPutCount']) > 0:
        print("failed to put {} messages.".format(response['FailedPutCount']))
        messages_num = 0
    messages_num -= current_batch_num
