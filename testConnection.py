import os
import boto3
from botocore.client import Config
from dotenv import load_dotenv
load_dotenv()

s3 = boto3.client('s3',
                  endpoint_url='http://127.0.0.1:9000',
                  aws_access_key_id=os.environ.get('MINIO_ACCESS_KEY'),
                  aws_secret_access_key=os.environ.get('MINIO_SECRET_KEY'),
                  config=Config(signature_version='s3v4'),
                  region_name='us-east-1')

folder_path = "tmp"
bucket_name = "huhu"
object_prefix = "huhu-folder/"

# s3.Bucket('huhu').upload_file('tmp/headphones-bronz.csv',"headphones-bronz.csv")
response = s3.list_objects_v2(Bucket=bucket_name, Prefix="huhu-folder/")
print(response)

# List all files in the folder
# for file_name in os.listdir(folder_path):
#     file_path = os.path.join(folder_path, file_name)
#     print(file_path)
#     # Upload each file to S3
#     with open(file_path, "rb") as f:
#         s3.upload_fileobj(f, bucket_name, file_name)
