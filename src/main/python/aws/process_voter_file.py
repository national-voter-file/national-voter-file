import boto3
import json
import urllib

batch_client = boto3.client('batch')
""":type: pyboto3.batch"""

s3_client = boto3.resource('s3')
""":type: pyboto3.s3"""


def lambda_handler(s3_event, context):
    print("Received event: " + json.dumps(s3_event, indent=2))

    bucket = s3_event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(s3_event['Records'][0]['s3']['object']['key'])

    state_code = find_bucket_state_tag(bucket)

    if state_code is not None:
        file_copy_job = submit_file_copy_job(batch_client, bucket, key)
        print("File Copy started: " + file_copy_job)

        transform_job_id = submit_transform_job(batch_client, state_code, key, file_copy_job)
        print("Transform submitted " + transform_job_id)
        return transform_job_id
    else:
        return "Can't determine state"


# Assume that the bucket has a tag called 'State' and it contains the two
# letter code for the state to process
def find_bucket_state_tag(bucket):
    try:
        tagging = s3_client.BucketTagging(bucket)
        tags = tagging.tag_set

        i = [el for el in tags if el['Key'] == 'State']
        if len(i) == 1:
            return([0]['Value'])

    except Exception:
        print("No State tags found for S3 bucket " + bucket)
        return None


def submit_file_copy_job(client, bucket, key):
    s3_path = "s3://%s/%s" % (bucket, key)
    command = {"command": ["sh", "-cxv", "aws s3 cp %s /work; chmod go+rw /work/%s" % (s3_path, key)]}

    job_submit_result = client.submit_job(jobName='CopyOHTestVoterFile', jobQueue='NVF',
                                          jobDefinition='S3Ops', containerOverrides=command)

    job_id = job_submit_result['jobId']
    return job_id


def submit_transform_job(client, state_name, filename, file_copy_job_id):
    command = {"command": ["-s", state_name, "--input_file", "/work/" + filename, "transform"]}

    transform_job_result = client.submit_job(jobName="Transform%s" % state_name, jobQueue='NVF',
                                             jobDefinition='ETL', containerOverrides=command,
                                             dependsOn=[{"jobId": file_copy_job_id}])
    return transform_job_result['jobId']


# Some test data -- need to move to nosetests
event = {"Records": [{"s3":
    {
        "object":
            {"key": "oh.csv"},
        "bucket": {"name": "foobar-12908349038"}}}]}
print(lambda_handler(event, None))
