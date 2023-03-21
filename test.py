#!/usr/bin/env/python3
import os
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv

# Lookup environment variables
load_dotenv()

# Lodookup environment variables
MINIO_ENDPOINT_VAR = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY_VAR = os.getenv('MINIO_ACCESS')
MINIO_SECRET_KEY_VAR = os.getenv('MINIO_SECRET')


def main():
  # Create a client with the MinIO server playground, its access key
  # and secret key.
  client = Minio(
    endpoint=MINIO_ENDPOINT_VAR,
    secure=False,
    access_key=MINIO_ACCESS_KEY_VAR,
    secret_key=MINIO_SECRET_KEY_VAR
  )

  bucket_name = "input"
  object_name = "challenge-text.zip"
  file_name = "/tmp/challenge-text.zip"
  
  found = client.bucket_exists(bucket_name)
  print(f'found {found}')
  if not found:
    client.make_bucket(bucket_name)
  else:
    print("Bucket already exists")

  result = client.fget_object(bucket_name, object_name, file_name)
  print("file result", result)


if __name__ == "__main__":
  try:
    main()
  except S3Error as exc:
    print("error occurred.", exc)
