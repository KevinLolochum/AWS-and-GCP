import os
import glob
import regex as re
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

# You will have to allow access on first run
credentials = GoogleCredentials.get_application_default()

# construct resource for interacting with API (returns an object with methods for interacting with API)
service = discovery.build('storage', 'v1', credentials=credentials)

# Change this filepath to fit new week 
filepath = r"filepath"
bucket = 'tgtvs'

# Create buckkett subdir
Week = "Subdir"


def Load(Files):
    " Inserts files into buckett "
    Files = glob.glob(filepath + "/*.csv")
    for file in Files:
        # Add subdictory by combining filename with subdir name
        body = {'name': Week+str(file).split("\\")[-1]}
        req = service.objects().insert(bucket=bucket, body=body, media_body=file)
        resp = req.execute()

    return


if __name__ ==  "__main__":
    Load()