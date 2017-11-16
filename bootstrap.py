import boto3
import json
import time


def has_bucket(bucket_name):
    try:
        client = boto3.client("s3")
        client.head_bucket(Bucket=bucket_name)
        return True
    except:
        return False


def create_bucket(bucket_name):
    client = boto3.client("s3")
    response = client.create_bucket(ACL='private', Bucket=bucket_name)
    return True


def get_account_id():
    client = boto3.client("sts")
    account_id = client.get_caller_identity()["Account"]
    return account_id


def create_role(role_name, account_id):
    trust_policy = {
      "Version": "2012-10-17",
      "Statement": [
	{
	  "Sid": "",
	  "Effect": "Allow",
	  "Principal": {
	    "Service": "firehose.amazonaws.com"
	  },
	  "Action": "sts:AssumeRole",
	  "Condition": {
	    "StringEquals": {
	      "sts:ExternalId": account_id
	    }
	  }
	}
      ]
    }
    firehose_policy = {
	"Version": "2012-10-17",
	"Statement": [
	    {
		"Sid": "",
		"Effect": "Allow",
		"Action": [
		    "s3:AbortMultipartUpload",
		    "s3:GetBucketLocation",
		    "s3:GetObject",
		    "s3:ListBucket",
		    "s3:ListBucketMultipartUploads",
		    "s3:PutObject"
		],
		"Resource": [
		    "arn:aws:s3:::*",
		    "arn:aws:s3:::*/*",
		    "arn:aws:s3:::%FIREHOSE_BUCKET_NAME%",
		    "arn:aws:s3:::%FIREHOSE_BUCKET_NAME%/*"
		]
	    },
	    {
		"Sid": "",
		"Effect": "Allow",
		"Action": [
		    "lambda:InvokeFunction",
		    "lambda:GetFunctionConfiguration"
		],
		"Resource": "arn:aws:lambda:us-east-1:{}:function:*".format(account_id)
	    },
	    {
		"Sid": "",
		"Effect": "Allow",
		"Action": [
		    "logs:PutLogEvents"
		],
		"Resource": [
		    "arn:aws:logs:us-east-1:{}:log-group:/aws/kinesisfirehose/*:log-stream:*".format(account_id)
		]
	    },
	    {
		"Sid": "",
		"Effect": "Allow",
		"Action": [
		    "kinesis:DescribeStream",
		    "kinesis:GetShardIterator",
		    "kinesis:GetRecords"
		],
		"Resource": "arn:aws:kinesis:us-east-1:{}:stream/%FIREHOSE_STREAM_NAME%".format(account_id)
	    },
	    {
		"Effect": "Allow",
		"Action": [
		    "kms:Decrypt",
		    "kms:DescribeKey",
		    "kms:GenerateDataKey"
		],
		"Resource": [
		    "arn:aws:kms:region:accountid:key/DUMMY_KEY_ID"
		],
		"Condition": {
		    "StringEquals": {
			"kms.ViaService": "kinesis.%REGION_NAME%.amazonaws.com"
		    },
		    "StringLike": {
			"kms:EncryptionContext:aws:kinesis:arn": "arn:aws:kinesis:%REGION_NAME%:{}:stream/%FIREHOSE_STREAM_NAME%".format(account_id)
		    }
		}
	    }
	]
    }

    client = boto3.client("iam")
    _ = client.create_role(
        Path='/',
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps(trust_policy),
        Description='Firehose delivery role'
    )
    _ = client.put_role_policy(
        RoleName=role_name,
        PolicyName='firehose_delivery_policy',
        PolicyDocument=json.dumps(firehose_policy)
    )
    return True



def create(params):
    bucket_name = params['bucket_name']
    role_name = params["role"]
    if not has_bucket(bucket_name):
        create_bucket(bucket_name)
        print("Created bucket: {}".format(bucket_name))
    bucket = get_bucket(bucket_name)
    if not has_role(role_name):
        result = create_role(role_name, params["account_id"])
        if result:
            print("Created role: {}".format(role_name))
            print("Waiting 10 seconds to role {} propagate through AWS".format(role_name))
            time.sleep(10)

    bucket.wait_until_exists()
    stream_name = params["stream_name"]
    stream_arn = create_delivery_stream(
        stream_name, bucket_name, params["batch_size"], params["batch_period"],
        params["compression"], params["prefix"], role_name)
    return stream_arn


def get_bucket(bucket_name):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    return bucket


def has_role(role_name):
    try:
        iam = boto3.resource('iam')
        role = iam.Role(role_name)
        role.load()
        return True
    except:
        return False


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
    "name": "account_id",
    "prompt_name": "account_id",
    "default": lambda: get_account_id()
}, {
    "name": "bucket_name",
    "prompt_name": "bucket name",
    "default": None
}, {
    "name": "stream_name",
    "prompt_name": "stream name",
    "default": None
}, {
    "name": "role",
    "prompt_name": "role name",
    "default": None
}, {
    "name": "batch_period",
    "prompt_name": "log file rotation period, in seconds [60-280]",
    "verify": lambda x: isInt(x) and int(x) >= 60 and int(x) <= 280,
    "default": 60
}, {
    "name": "batch_size",
    "prompt_name": "log file size in MB [5-128]",
    "verify": lambda x: isInt(x) and int(x) >= 5 and int(x) <= 128,
    "default": 128
}, {
    "name": "compression",
    "prompt_name":
    "compression, one of 'UNCOMPRESSED'|'GZIP'|'ZIP'|'Snappy'",
    "verify":
    lambda x: x in ['UNCOMPRESSED', 'GZIP', 'ZIP', 'Snappy'],
    "default":
    "GZIP"
}, {
    "name": "prefix",
    "prompt_name": "S3 bucket prefix",
    "default": "raw/"
}]


def transform_user_input(parameters):
    return {p['name']: p['value'] for p in parameters}


def get_user_input(param):
    default = param["default"]() if callable(param["default"]) else param["default"]
    value = input(
        "Enter the {} [{}]: ".format(param["prompt_name"], default))
    if value != "" and "verify" in param and not param["verify"](value):
        return False
    if value == "":
        param["value"] = default
    else:
        param["value"] = value.strip()
    return param["value"] is not None


if __name__ == "__main__":
    for param in parameters:
        while not get_user_input(param):
            pass

    parameters = transform_user_input(parameters)
    r = create(parameters)
    print(r)
