#!/usr/bin/env/python3
import os
import json
import zipfile
import io

from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv

# Lookup environment variables
load_dotenv()

# Lodookup environment variables
MINIO_ENDPOINT_VAR = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY_VAR = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY_VAR = os.getenv('MINIO_SECRET_KEY')

BUCKET_NAME = "input"
UNPACKED_BUCKET_NAME = "unpacked"
DOWNLOADED_FILE_NAME = "temp.zip"

# Create a client with the MinIO server playground, its access key
# and secret key.
client = Minio(
    endpoint=MINIO_ENDPOINT_VAR,
    access_key=MINIO_ACCESS_KEY_VAR,
    secret_key=MINIO_SECRET_KEY_VAR,
    secure=False
)

def main():
  
    #   found = client.bucket_exists(bucket_name)
    #   print(f'found {found}')
    #   if not found:
    #     client.make_bucket(bucket_name)
    #   else:
    #     print("Bucket already exists")

    #   result = client.fget_object(bucket_name, object_name, file_name)
    #   print("file result", result)

    json_string = '{"EventName":"s3:ObjectCreated:Put","Key":"input/challenge-text.zip","Records":[{"eventVersion":"2.0","eventSource":"minio:s3","awsRegion":"","eventTime":"2023-03-21T10:00:34.001Z","eventName":"s3:ObjectCreated:Put","userIdentity":{"principalId":"minio"},"requestParameters":{"principalId":"minio","region":"","sourceIPAddress":"127.0.0.1"},"responseElements":{"content-length":"0","x-amz-request-id":"174E66E27CB352E4","x-minio-deployment-id":"c8654402-5764-4f57-9980-bb254c054a4d","x-minio-origin-endpoint":"http://172.17.55.254:9000"},"s3":{"s3SchemaVersion":"1.0","configurationId":"Config","bucket":{"name":"input","ownerIdentity":{"principalId":"minio"},"arn":"arn:aws:s3:::input"},"object":{"key":"challenge-text.zip","size":863,"eTag":"a4c672349820e84cec1be9bf0fc2dfdf","contentType":"application/zip","userMetadata":{"content-type":"application/zip"},"sequencer":"174E66E27D03C84E"}},"source":{"host":"127.0.0.1","port":"","userAgent":"MinIO (linux; amd64) minio-go/v7.0.47 MinIO Console/(dev)"}}]}'

    #convert string to  object
    json_body = json.loads(json_string)

    #check new data type
    print(type(json_body))


    print(f'received: {json_body}')
    # event = json.dumps(body.decode())
    records = []

    # process each record in the event
    if 'EventName' in json_body and json_body['EventName'] == 's3:ObjectCreated:Put':
        for record in json_body['Records']:
            BUCKET_NAME = record['s3']['bucket']['name']
            key = record['s3']['object']['key']

            try:
                # download the zip file
                client.fget_object(BUCKET_NAME, key, DOWNLOADED_FILE_NAME)
                print(f'downloaded {key} from {BUCKET_NAME}')
                
                print(f'Unzipped file')

                # # unzip each file in memory
                unzipped = []

                with zipfile.ZipFile(DOWNLOADED_FILE_NAME) as archive:
                    archive.extractall()
                    for file in archive.namelist():
                        print(file)

                        # post file to minio in its own folder
                        client.fput_object(UNPACKED_BUCKET_NAME, 'newfolder/' + file, file)

                        unzipped.append({'id': str(uuid.uuid4()), 'key': 'newfolder/' + file, 'bucket': UNPACKED_BUCKET_NAME})

                # update event
                record['unzipped'] = unzipped
                records.append(record)
                    
            except Exception as e:
                print(f'error processing key [{key}] from bucket [{BUCKET_NAME}] - {e}')

if __name__ == "__main__":
  try:
    main()
  except S3Error as exc:
    print("error occurred.", exc)
