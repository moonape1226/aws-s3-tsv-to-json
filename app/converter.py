import csv
import gzip
import json
import sys
import os
import boto3

global SORUCE_BUCKET_NAME
global DIST_BUCKET_NAME
global LINE_PER_FILE
global DB_NAME
global TABLE_NAME
global DATE

def generateOutFileName(upload_file_prefix, num_line, LINE_PER_FILE):

    return upload_file_prefix + '_' + str(int(num_line / LINE_PER_FILE)) + '.json.gz'

def saveJSONFile(out_file, json_list, num_line):

    with gzip.open(out_file, 'wt', encoding='UTF-8') as gzip_out:
        gzip_out.write('\n'.join(json_list))

def uploadJSONToS3(s3_client, upload_path, out_file):

    filename = out_file
    s3_client.upload_file('./' + filename, DIST_BUCKET_NAME, upload_path + filename)

def transformToJSON(in_file, upload_path, upload_file_prefix, LINE_PER_FILE, s3_client):

    with gzip.open(in_file, 'rt', encoding='UTF-8') as gzip_in:
        first_line = gzip_in.readline().strip()
        titles = first_line.split('\t')

        json_dict, json_list = {}, []
        num_line = 0

        for line in gzip_in:
            line = line.strip()

            for key, value in zip(titles, line.split('\t')):
                json_dict[key] = value

            json_list.append(str(json_dict))
            json_dict = {}

            if num_line != 0 and num_line % LINE_PER_FILE == 0:
                out_file = generateOutFileName(upload_file_prefix, num_line, LINE_PER_FILE)
                saveJSONFile(out_file, json_list, num_line)
                uploadJSONToS3(s3_client, upload_path, out_file)
                json_list.clear()

            num_line += 1

        if num_line % LINE_PER_FILE != 0:
            out_file = generateOutFileName(upload_file_prefix, num_line, LINE_PER_FILE)
            saveJSONFile(out_file, json_list, num_line)
            uploadJSONToS3(s3_client, upload_path, out_file)
            json_list.clear()

if __name__ == '__main__':

    SORUCE_BUCKET_NAME   = 'kkbox-db-dump'
    DIST_BUCKET_NAME     = 'kkbox-json-dump'
    LINE_PER_FILE        = 1000000
    AWS_ACCESS_KEY_ID       = ''
    AWS_SECRET_ACCESS_KEY   = ''

    if "DB_NAME" in os.environ:
        DB_NAME = os.environ["DB_NAME"]
    else:
        sys.exit(-1)

    if "DB_TABLE" in os.environ:
        DB_TABLE = os.environ["DB_TABLE"]
    else:
        sys.exit(-1)

    if "DATE" in os.environ:
        DATE = os.environ["DATE"]
    else:
        sys.exit(-1)

    if "AWS_ACCESS_KEY_ID" in os.environ:
        AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]

    if "AWS_SECRET_ACCESS_KEY" in os.environ:
        AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]

    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        s3_client = boto3.client('s3', aws_access_key_id = AWS_ACCESS_KEY_ID, aws_secret_access_key = AWS_SECRET_ACCESS_KEY)
    else:
        s3_client = boto3.client('s3')

    download_path = DB_NAME + '/' + DB_TABLE + '/' + DATE + '/'
    download_file = DB_NAME + '.' + DB_TABLE + '-' + DATE + '.tsv.gz'
    in_file       = './' + download_file
    s3_client.download_file(SORUCE_BUCKET_NAME, download_path + download_file, in_file)

    upload_path         = download_path
    upload_file_prefix  = DB_NAME + '.' + DB_TABLE + '-' + DATE

    transformToJSON(in_file, upload_path, upload_file_prefix, LINE_PER_FILE, s3_client)
