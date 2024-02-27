import logging
import boto3
from botocore.exceptions import ClientError
import os

import logging
import boto3
from botocore.exceptions import ClientError


def create_bucket(bucket_name, region=None):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """
    # Check if the bucket already exists
    s3 = boto3.client('s3')
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists")
        return True
    except ClientError as e:
        # If the bucket does not exist, create it
        pass

    # Create bucket
    try:
        if region is None:
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3', region_name=region)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def create_folder(bucket_name, folder_name):
    """
    Create a folder (prefix) in an S3 bucket if it does not already exist.

    Args:
        bucket_name (str): The name of the S3 bucket.
        folder_name (str): The name of the folder (prefix) to create.

    Returns:
        bool: True if the folder was created successfully or already exists, False otherwise.
    """
    s3 = boto3.client('s3')
    # Check if the folder already exists
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)

    # If the folder exists (objects are returned), return True
    if 'Contents' in response:
        print(f"Folder '{folder_name}' already exists in bucket '{bucket_name}'")
        return True

    # If the folder does not exist, create a dummy file to create it
    s3.put_object(Bucket=bucket_name, Key=(folder_name + 'dummyfile.txt'))
    print(f"Folder '{folder_name}' created in bucket '{bucket_name}'")
    return True

def list_buckets_and_folders():
    """
    List all buckets and folders (prefixes) inside each bucket in S3.

    Returns:
        dict: A dictionary where keys are bucket names and values are lists of folder names.
    """
    s3 = boto3.client('s3')

    # Get a list of all buckets
    response = s3.list_buckets()

    # Initialize a dictionary to store bucket names and their folders
    bucket_folders = {}

    # Iterate over each bucket
    for bucket_info in response['Buckets']:
        bucket_name = bucket_info['Name']

        # Get a list of objects in the bucket (folders are treated as objects)
        objects_response = s3.list_objects_v2(Bucket=bucket_name, Delimiter='/')

        # Extract folder names (prefixes) from the objects response
        folders = [prefix['Prefix'] for prefix in objects_response.get('CommonPrefixes', [])]

        # Store the bucket name and its folders in the dictionary
        bucket_folders[bucket_name] = folders

    for bucket, folders in bucket_folders.items():
        print(bucket)
        if folders:
            for folder in folders:
                print(f"\t{folder}")
        else:
            print("No folders found.")
        print()
    pass

# Upload to s3







if __name__ == "__main__":
    s3 = boto3.client('s3')

    # bucket_name = 'hehe-bucket'
    # folder_name = 'huhu-folder/'

    # #Create bucket if not exists 
    # create_bucket('hehe-bucket', 'us-west-1')

    # # Create folder if not exists
    # create_folder('hehe-bucket', 'huhu-folder/')

    # Retrieve the list of existing buckets
    # list_buckets_and_folders()

    #upload all csv in folder tmp to s3
    bucket_name = 'hehe-bucket'
    folder_name = 'huhu-folder/'
    # for file in files:
    #     with open(f'tmp/{file}', 'rb') as data:
    #         s3.upload_fileobj(data, bucket_name, folder_name + 'files')

    # print all file name in folder tmp
    folder_path = "tmp/"
    for file_name in os.listdir(folder_path):
        # Check if the path is a file (not a directory)
        if os.path.isfile(os.path.join(folder_path, file_name)):
            with open(os.path.join(folder_path, file_name), 'rb') as data:
                s3.upload_fileobj(data, bucket_name, folder_name + file_name)
                print(f"File '{file_name}' uploaded to '{bucket_name}/{folder_name}'")
    