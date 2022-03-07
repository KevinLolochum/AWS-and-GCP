# Imports
from msilib.schema import Class
import os, sys, io, json, glob, zipfile
from pathlib import Path
import regex as re


from google.cloud import storage
from google.cloud import bigquery
import GCSload as GCSL

# GCS solution https://github.com/googleapis/python-bigquery -- Codes under samples

# Project and dataset
PROJECT_ID = "Your_BQ_project_id"
DATASET = "Schema_name"

# Your Credentials file should be a JSON file
credential_path = r"Path\cred.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

# variables to be made table names in your schema
FILE_NAMES = ["Response", "Action", "NAT", "OTT"]

def load(BUCKET_NAME, FILE_LIST, storage_client, FILE_NAMES):
    ''' Function to Recursively load all files in the directory
        
        '''

    # Construct a BigQuery client object.
    bq_client = bigquery.Client()

    for filename in FILE_LIST:
        if (".csv" in filename) or (".txt" in filename):
            print("Loading file: "+str(filename))
            bqschema = get_schema(filename)

            #Give table names based on their filenames
            for FILE_NAME in FILE_NAMES:
                if FILE_NAME in str(filename.split("/")[-1].split(".")[0]):
                    TABLE = FILE_NAME
                else:
                    continue # ignore and go to the next file or
                    #print(f"FILE_NAME {FILE_NAME} doesn't match any schema")
                    #sys.exit()
                print(filename)
                print(TABLE)
            # Set table_id to the ID of the table to create.
            table_id = PROJECT_ID+"."+DATASET+"."+TABLE
            print("Creating table: "+table_id)

            job_config = bigquery.LoadJobConfig(
                schema=bqschema,
                skip_leading_rows=1,
                source_format=bigquery.SourceFormat.CSV,
                allow_jagged_rows=True,
                ignore_unknown_values=True,
                allow_quoted_newlines=False,
                autodetect=False,
                field_delimiter=",",
                max_bad_records=1000,
                write_disposition="WRITE_TRUNCATE",
                destination_table_description="Automatically updated"
            )
            uri = "gs://"+BUCKET_NAME+"/"+filename

            # Load your table
            load_job = bq_client.load_table_from_uri(
                uri, table_id, job_config=job_config
            )  # api request


            load_job.result()  # Waits for the job to complete.

            #Check if table is loaded
            destination_table = bq_client.get_table(table_id)  # Make an API request.
            print("Loaded {} rows.".format(destination_table.num_rows))


def get_schema(filename):

    schema_dir = os.getcwd()+"/TVSschema/*.txt" # Your files dir
    schema_files = glob.glob(schema_dir)

    for c in range(0, len(schema_files)):
        schema_file = schema_files[c]
        basename = os.path.basename(schema_file)
        
        if (str(basename.split(".")[0]).upper() in str(filename).upper()):
            # use this schema
            break
        if (c == len(schema_files)-1):
            # Not that schema was not found and proceed to the next
            print("Schema not found for file: "+str(filename))
            continue
            #sys.exit(1)

    with open(Path(schema_file)) as f:
        schema_data = f.readlines()
    print("Loaded schema: "+str(basename))

    schema = []
    # Get lines in schema template files (from you dir) use to create table later
    for line in schema_data:
        field_name = line.split(":")[0]
        field_type = line.split(":")[1]
        schema.append(bigquery.SchemaField(field_name, field_type.strip()[:-1], mode="NULLABLE"))
    
    return schema

# This can be a function as well
class main:
    FILE_LIST = []
    args = sys.argv
    # Require only three sys args that you pass
    if (len(args) < 3) or (len(args) > 3):
        print("ERROR!\nUsage: python BQload.py bucketname directory_of_files")
        print("Example: python BQload.py buckett Subdir")
        sys.exit(1)
    else:
        BUCKET_NAME = str(args[1]).strip()
        FILE_DIR = str(args[2]).strip()

        # get list of files to load 
        storage_client = storage.Client()
        blobs = storage_client.list_blobs(BUCKET_NAME, prefix=FILE_DIR)
        for blob in blobs:
            FILE_LIST.append(blob.name)

        #Run the load buckett function
        load(BUCKET_NAME, FILE_LIST, storage_client, FILE_NAMES)

if __name__ == "__main__":
    GCSL.Load()
    main()
