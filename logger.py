import json
import logging
import datetime
import boto3


class KinesisLogger():
    def __init__(self, stream_name):
        self.stream = stream_name
        self.firehose = boto3.client('firehose')

    def error(self, message):
        message["Level"] = "ERROR"
        self.log(message)

    def warning(self, message):
        message["Level"] = "WARNING"
        self.log(message)

    def info(self, message):
        message["Level"] = "INFO"
        self.log(message)

    def augment_message(self, message):
        message["Timestamp"] = datetime.datetime.utcnow().isoformat()

    def log(self, message):
        self.augment_message(message)
        try:
            record = {'Data': bytes(json.dumps(message) + '\n', 'utf-8')}
            self.firehose.put_record(
                DeliveryStreamName=self.stream, Record=record)
        except Exception as e:
            logging.error(e)

    def log_batch(self, messages):
        try:
            messages_num = len(messages)
            for message in messages:
                self.augment_message(message)

            while messages_num > 0:
                current_batch_num = min(messages_num, 500)
                records = [{
                    'Data': bytes(json.dumps(message) + '\n', 'utf-8')
                } for message in messages]
                response = self.firehose.put_record_batch(
                    DeliveryStreamName=self.stream, Records=records)
                if int(response['FailedPutCount']) > 0:
                    logging.error("failed to put {} messages.".format(
                        response['FailedPutCount']))
                messages_num -= current_batch_num
        except Exception as e:
            logging.error(e)


if __name__ == "__main__":
    logger = KinesisLogger("datapao-logging-6")
    logger.log({"name": "mate"})
    logger.log_batch([{
        "name": "mate"
    }, {
        "origin": "testing",
        "timestamp": 1391203123
    }])
