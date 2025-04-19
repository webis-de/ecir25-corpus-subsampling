#!/usr/bin/env python3
from ray import init, remote, get
from tqdm import tqdm
from pathlib import Path
from glob import glob
import json
import time

def load_allowlist():
    ret = []
    for i in glob("data/*"):
        ret += json.loads(Path(i).read_text())
    print('->', len(ret))
    print('->', len(set(ret)))
    return set(ret)

def aws_credentials():
    import os
    ret = []

    for i in ['AWS_ACCESS_KEY', 'AWS_SECRET']:
        if i not in os.environ:
            print(f'Please pass {i} as environment variable.')
            raise ValueError(f'Please pass {i} as environment variable.')
        ret += [os.environ[i]]
    return ret + ['http://s3.dw.webis.de:7480/']


CREDS = None

def create_s3_client():
    import boto3
    global CREDS
    if CREDS is None:
        KEY, SECRET, ENDPOINT = aws_credentials()
        CREDS = [KEY, SECRET, ENDPOINT]
    else:
        KEY, SECRET, ENDPOINT = CREDS

    session = boto3.session.Session(KEY, SECRET)

    return session.client(
        service_name='s3',
        endpoint_url=ENDPOINT,
        config=boto3.session.Config(signature_version='s3v4')
    )

def get_bucket_files(s3_client, bucket):
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket)
    
    return [obj['Key'] for page in pages for obj in page['Contents']]

def stream_data_from_s3(bucket, file):
    s3_client = create_s3_client()
    response = s3_client.get_object(Bucket=bucket, Key=file)

    return response['Body']

def yield_record(bucket, file, start_offset, end_offset):
    s3_client = create_s3_client()
    response = s3_client.get_object(Bucket=bucket, Key=file, Range=f"bytes={start_offset}-{end_offset}")
    file = Path("/mnt/ceph/storage/data-in-progress/data-research/web-search/lsr-benchmark/clueweb09") / file
    if not file.is_file():
        file.parent.mkdir(exist_ok=True, parents=True)
    with open(file, 'ab+') as f:
        f.write(response['Body'].read())

@remote
def stream_file(bucket, file, allow_list):
    data = stream_data_from_s3(bucket, file)
    from fastwarc.stream_io import GZipStream
    from fastwarc.warc import ArchiveIterator
    start_offset = None
    for record in ArchiveIterator(GZipStream(stream_data_from_s3(bucket, file))):
        if start_offset is not None:
            yield_record(bucket, file, start_offset, record.stream_pos -1)

        trec_id = None
        for k, v in record.headers:
            if k == 'WARC-TREC-ID':
                trec_id = v
        if not trec_id:
            continue
        if trec_id in allow_list:
            start_offset = record.stream_pos
        else:
            start_offset = None

    if start_offset is not None:
        yield_record(bucket, file, start_offset, start_offset*2)
    return file

def chunk_array(arr, chunk_size=50):
    return [arr[i:i + chunk_size] for i in range(0, len(arr), chunk_size)]

DATASETS = [
    'corpus-clueweb09-recompressed',
#    'corpus-clueweb12-recompressed',
]

if __name__ == '__main__':
    init()
    allowlist = load_allowlist()
    s3_client = create_s3_client()
    DATASET_TO_FILES = {i: set() for i in DATASETS}
    for dataset in DATASETS:
        for i in get_bucket_files(s3_client, dataset):
            DATASET_TO_FILES[dataset].add(i)

    processed_files = set(Path("data/processed_files.txt").read_text().split('\n'))

    for dataset in DATASETS:
        print(dataset, '->', len(DATASET_TO_FILES[dataset]))
        for chunk in tqdm(chunk_array(DATASET_TO_FILES[dataset])):
            streams = []
            for file in chunk:
                if file not in processed_files:
                    streams.append(stream_file.remote(dataset, file, allowlist))

                if len(streams) > 0:
                    with open("data/processed_files.txt", "at+") as f:
                        for i in streams:
                            f.write(get(i) + '\n')

