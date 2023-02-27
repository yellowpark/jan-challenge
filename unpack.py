#!/usr/bin/env/python
from minio import Minio
from minio.error import S3Error



def main():
  # Create a client with the MinIO server playground, its access key
  # and secret key.
  client = Minio(
    endpoint='minio-chris-d.console-openshift-console.flows-dev-cluster-7c309b11fc78649918d0c8b91bcb5925-0000.eu-gb.containers.appdomain.cloud',
    secure=False,
    access_key='RknDK8Uv2psL78hi',
    secret_key='TlEkW1rah7uTtJPuDrh5kdbj2Bd8pxYX'
  )

  bucket_name = "input"
  object_name = "challenge-text.zip"
  file_name = "/tmp/challenge-text.zip"
  
  found = client.bucket_exists(bucket_name)
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
