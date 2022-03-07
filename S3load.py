# Imports
from boto import connect_s3
from boto.s3.connection import Location
import sys, boto
from boto.s3.key import Key
import glob


# Add access key ID and Access Key
# You have to create this in IAM roles access management
AWS_ACCESS_KEY_ID = 'Your access key'
AWS_SECRET_ACCESS_KEY = 'Your secret acess key'

bucket_name = 'Your buckett name'
conn = connect_s3(AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY)

# Create s3 buckett
bucket = conn.create_bucket(bucket_name,
    location=Location.DEFAULT)

Path = r"filepath"  #Depending on where your files are

#load
k = Key(bucket)

def Load():
    Files = glob.glob(Path + "/*.csv")
    for File in Files:
        print (f"Uploading {File} to Amazon S3 bucket {bucket_name}")
        k.key = str(File).split("\\")[-1] # use the filepath last path as name
        k.set_contents_from_filename(File)


if __name__ == "__main__":
    Load()
