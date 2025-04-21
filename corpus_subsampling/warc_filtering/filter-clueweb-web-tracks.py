#!/usr/bin/env python3
from ray import init, remote, get
from tqdm import tqdm
from pathlib import Path
from io import BytesIO
from glob import glob
import json
import time
import click
import gzip

def load_allowlist():
    ret = []
    for i in glob("data/*.json"):
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

def get_bucket_files(s3_client, dataset):
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=f"corpus-{dataset}-recompressed")
    
    return [obj['Key'] for page in pages for obj in page['Contents']]

def stream_data_from_s3(dataset, file):
    s3_client = create_s3_client()
    response = s3_client.get_object(Bucket=f"corpus-{dataset}-recompressed", Key=file)

    return response['Body']

OUT_DIR = Path("/mnt/ceph/storage/data-in-progress/data-research/web-search/lsr-benchmark/")

def meta_file(file, dataset):
    return OUT_DIR / dataset / (file.replace(".warc.gz", ".jsonl"))

def bytes_of_warc_record_from_s3(dataset, file, start_offset, end_offset):
    s3_client = create_s3_client()
    ret = s3_client.get_object(Bucket=f"corpus-{dataset}-recompressed", Key=file, Range=f"bytes={start_offset}-{end_offset}")
    return ret['Body'].read()

def yield_record(dataset, file, start_offset, end_offset, trec_id):
    if not (OUT_DIR / dataset / file).is_file():
        (OUT_DIR / dataset /file).parent.mkdir(exist_ok=True, parents=True)

    with open(meta_file(file, dataset), 'at+') as f:
        f.write(json.dumps({"trec_id": trec_id, "bucket": f"corpus-{dataset}-recompressed", "file": file, "start_offset": start_offset, "end_offset": end_offset}) + '\n')

def md5_sum(b):
    from hashlib import md5
    return str(md5(b).hexdigest())

@remote
def stream_file(dataset, file, allow_list):
    if (OUT_DIR / dataset / file).is_file():
        (OUT_DIR / dataset / file).unlink()
    if meta_file(file, dataset).is_file():
        meta_file(file, dataset).unlink()

    data = stream_data_from_s3(dataset, file)
    from fastwarc.stream_io import GZipStream
    from fastwarc.warc import ArchiveIterator
    start_offset = None
    prev_trec_id = None
    for record in ArchiveIterator(GZipStream(stream_data_from_s3(dataset, file))):
        if start_offset is not None:
            yield_record(dataset, file, start_offset, record.stream_pos -1, prev_trec_id)

        trec_id = None
        for k, v in record.headers:
            if k == 'WARC-TREC-ID':
                trec_id = v
        if not trec_id:
            continue
        if trec_id in allow_list:
            start_offset = record.stream_pos
            prev_trec_id = trec_id
        else:
            start_offset = None
            prev_trec_id = None

    if start_offset is not None:
        yield_record(dataset, file, start_offset, start_offset*2, prev_trec_id)
    return file

def chunk_array(arr, chunk_size=150):
    return [arr[i:i + chunk_size] for i in range(0, len(arr), chunk_size)]

def step_01_access_files(dataset):
    init()
    allowlist = load_allowlist()
    processed_files = set((OUT_DIR / dataset / "processed_files.txt").read_text().split('\n'))
    s3_client = create_s3_client()
    files = set()
    skipped = 0
    for i in get_bucket_files(s3_client, dataset):
        if i not in processed_files:
            files.add(i)
        else:
            skipped += 1
    print(f"Skip {skipped} already processed files")


    print(dataset, '->', len(files))
    for chunk in tqdm(chunk_array(list(files))):
        streams = []
        for file in chunk:
            streams.append(stream_file.remote(dataset, file, allowlist))

        if len(streams) > 0:
            with open(OUT_DIR / dataset / "processed_files.txt", "at+") as f:
                for i in streams:
                    f.write(get(i) + '\n')
                    f.flush()

def load_all_access_files(dataset):
    ret = []
    for d in ["", "**", "**/**", "**/**/**", "**/**/**/**", "**/**/**/**/**", "**/**/**/**/**/**", "**/**/**/**/**/**/**"]:
        ret.extend(glob(f"{OUT_DIR}/{dataset}/{d}/*.jsonl"))
    return ret

def partition_access_files(files):
    all_size = 0
    current_index = 0
    ret = {}
    for f in tqdm(files, "Partition access files"):
        lines = [json.loads(i) for i in Path(f).read_text().split('\n') if i]
        if current_index not in ret:
            ret[current_index] = []
        for l in lines:
            all_size += l['end_offset'] - l["start_offset"]
            ret[current_index].append(l)
        if all_size > (1024*1024*1024):
            all_size = 0
            current_index += 1
    return ret

@remote
def persist_warcs_into_file(output_file, dataset, files):
    with open(output_file, "wb") as output_warc:
        for f in files:
            warc_bytes = bytes_of_warc_record_from_s3(dataset, f["file"], f["start_offset"], f["end_offset"])
            md5 = md5_sum(warc_bytes)
            assert md5 == md5_sum(warc_bytes)
            output_warc.write(warc_bytes)
    return output_file

def persist_filtered_warcs(partitions, dataset):
    print(f"\nPersist {len(partitions)} partitions:\n\n")
    init()
    jobs = []
    with gzip.open(f"{OUT_DIR}/{dataset}/filtered/documents.jsonl.gz", "wt") as output_file:
        for partition in tqdm(partitions, "start partitions"):
            offset = 0
            file_name = f"{dataset}-trec-filtered-0{partition}.warc.gz"
            output_warc_file = f"{OUT_DIR}/{dataset}/filtered/{file_name}"

            for f in partitions[partition]:
                size = f["end_offset"] - f["start_offset"]
                meta = {"trec_id": f["trec_id"], "source": {k: f[k] for k in ["bucket", "file", "start_offset", "end_offset"]}, "file": file_name, "start_offset": offset, "end_offset": offset + size}
                output_file.write(json.dumps(meta) + "\n")
                offset += size + 1

            jobs.append(persist_warcs_into_file.remote(output_warc_file, dataset, partitions[partition]))
    print([get(i) for i in jobs])

def step_02_persist_files(dataset):
    files = load_all_access_files(dataset)
    partitions = partition_access_files(files)
    persist_filtered_warcs(partitions, dataset)


@click.command()
@click.argument("step", type=click.Choice(["01-access-files", "02-persist-files"]))
@click.argument("dataset", type=click.Choice(["clueweb09", "clueweb12"]))
def main(step, dataset):
    if step == "01-access-files":
        print(f"Use s3 client {create_s3_client()}")
        step_01_access_files(dataset)
    elif step == "02-persist-files":
        print(f"Use s3 client {create_s3_client()}")
        step_02_persist_files(dataset)
    else:
        raise ValueError(f"Unknown step '{step}'.")

if __name__ == '__main__':
    main()
