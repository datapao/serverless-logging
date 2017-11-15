import boto3


def has_bucket(bucket_name):
    try:
        client.head_bucket(Bucket=bucket_name)
        return True
    except:
        pass
    return False


def create_bucket(bucket_name):
    client = boto3.client("s3")
    bucket_configuration = {}
    response = client.create_bucket(ACL='private', Bucket=bucket_name)
    return response


def create(params):
    bucket_name = params['bucket_name']
    if not has_bucket(bucket_name):
        create_bucket(bucket_name)
    bucket = get_bucket(bucket_name)
    bucket.wait_until_exists()
    stream_name = params["stream_name"]
    stream_arn = create_delivery_stream(
        stream_name, bucket_name, params["batch_size"], params["batch_period"],
        params["compression"], params["prefix"], params["role"])
    return stream_arn


def get_bucket(bucket_name):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    return bucket


def get_role_arn(role_name):
    iam = boto3.resource('iam')
    role = iam.Role(role_name)
    return role.arn


def create_delivery_stream(stream_name, bucket_name, size, seconds,
                           compression, prefix, role_name):
    client = boto3.client("firehose")
    bucket_arn = "arn:aws:s3:::{}".format(bucket_name)
    role_arn = get_role_arn(role_name)
    response = client.create_delivery_stream(
        DeliveryStreamName=stream_name,
        DeliveryStreamType='DirectPut',
        S3DestinationConfiguration={
            'RoleARN': role_arn,
            'BucketARN': bucket_arn,
            'Prefix': prefix,
            'BufferingHints': {
                'SizeInMBs': size,
                'IntervalInSeconds': seconds
            },
            'CompressionFormat': compression
        })
    return response['DeliveryStreamARN']


# Create Firehose
def isInt(value):
    try:
        int(value)
        return True
    except ValueError:
        return False


parameters = [{
    "name": "bucket_name",
    "prompt_name": "bucket name",
    "value": None
}, {
    "name": "stream_name",
    "prompt_name": "stream name",
    "value": None
}, {
    "name": "role",
    "prompt_name": "role name",
    "value": None
}, {
    "name": "batch_period",
    "prompt_name": "log file rotation period, in seconds [60-280]",
    "verify": lambda x: isInt(x) and int(x) >= 60 and int(x) <= 280,
    "value": 60
}, {
    "name": "batch_size",
    "prompt_name": "log file size in MB [5-128]",
    "verify": lambda x: isInt(x) and int(x) >= 5 and int(x) <= 128,
    "value": 128
}, {
    "name":
    "compression",
    "prompt_name":
    "compression, one of 'UNCOMPRESSED'|'GZIP'|'ZIP'|'Snappy'",
    "verify":
    lambda x: x in ['UNCOMPRESSED', 'GZIP', 'ZIP', 'Snappy'],
    "value":
    "GZIP"
}, {
    "name": "prefix",
    "prompt_name": "S3 bucket prefix",
    "value": "raw/"
}]


def transform_user_input(parameters):
    return {p['name']: p['value'] for p in parameters}


def get_user_input(param):
    value = input(
        "Enter the {} [{}]: ".format(param["prompt_name"], param["value"]))
    if value != "" and "verify" in param and not param["verify"](value):
        return False
    if value != "":
        param["value"] = value
    return param["value"] != None


if __name__ == "__main__":
    for param in parameters:
        while not get_user_input(param):
            pass

    parameters = transform_user_input(parameters)
    r = create(parameters)
    print(r)
