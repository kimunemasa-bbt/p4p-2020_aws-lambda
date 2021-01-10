import json
import urllib.parse
import boto3
import os

print('Loading function')

s3 = boto3.client('s3')
rekognition = boto3.client('rekognition', region_name=os.environ['AWS_REGION'])

def rekog_detect_labels(key):
    response = rekognition.detect_labels(
        Image={
            'S3Object':{
                'Bucket':os.environ['BUCKET'],
                'Name':key
            }
        }, MaxLabels=10) 

    label = []
    for row in response['Labels']:
        label.append(row['Name'])
    label_str = '-'.join(label)
    return label_str


def put_tags(key, tags):
    put_obj = s3.put_object_tagging(
        Bucket=os.environ['BUCKET'],
        Key=key,
        Tagging={
            'TagSet': [
                { 'Key': 'element', 'Value': tags }
            ]
        }
    )


def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    # bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        tags = rekog_detect_labels(key)
        put_tags(key, tags)
    except Exception as e:
        print(e)
        raise e
