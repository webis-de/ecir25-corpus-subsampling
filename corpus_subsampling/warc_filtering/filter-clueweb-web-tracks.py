#!/usr/bin/env python3
try:
    from ray import init, remote, get
except:
    print('no ray is available...')
    def remote(func):
        def no_ray_installed():
            raise ValueError("no ray installed...")
        return no_ray_installed
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

def get_size_of_warc_file_from_s3(dataset, file):
    s3_client = create_s3_client()
    response = s3_client.head_object(Bucket=f"corpus-{dataset}-recompressed", Key=file)
    return int(response['ContentLength'])

OUT_DIR = Path("/mnt/ceph/storage/data-in-progress/data-research/web-search/lsr-benchmark/")

def meta_file(file, dataset):
    return OUT_DIR / dataset / (file.replace(".warc.gz", ".jsonl"))

def bytes_of_warc_record_from_s3(dataset, file, start_offset, end_offset):
    for i in range(5):
        try:
            s3_client = create_s3_client()
            ret = s3_client.get_object(Bucket=f"corpus-{dataset}-recompressed", Key=file, Range=f"bytes={start_offset}-{end_offset}")
            ret = ret['Body'].read()

            if len(ret) <= (((end_offset - start_offset) +1) - 100):
                raise ValueError(f"Response has unexpected length. Expected {(end_offset - start_offset) + 1}. Got {len(ret)}.")

            return ret
        except Exception as e:
            if i == 4:
                raise e
            else:
                time.sleep(5)

def bytes_of_warc_record_from_local_file(dataset, file, start_offset, end_offset):
    with open(f"{OUT_DIR}/{dataset}/filtered/{file}", "rb") as f:
        f.seek(start_offset)
        ret = f.read((end_offset - start_offset) + 1)
        if len(ret) != ((end_offset - start_offset) + 1):
            raise ValueError("foo")
        return ret

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
        yield_record(dataset, file, start_offset, get_size_of_warc_file_from_s3(dataset, file), prev_trec_id)
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

def partition_access_files(files, dataset):
    result_file = Path(f'data/{dataset}-partitions.json.gz')
    if not result_file.is_file():

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
            if all_size > (250*1024*1024):
                all_size = 0
                current_index += 1

        with gzip.open(result_file, 'wt') as f:
            f.write(json.dumps(ret))

    with gzip.open(result_file, 'rt') as f:
        return json.loads(f.read())

def persist_warcs_into_file(output_file, dataset, files):
    persisted_files = []

    with open(output_file, "wb") as output_warc:
        all_size = 0
        for f in tqdm(files):
            warc_bytes = bytes_of_warc_record_from_s3(dataset, f["file"], f["start_offset"], f["end_offset"])
            md5 = md5_sum(warc_bytes)
            assert md5 == md5_sum(warc_bytes)
            output_warc.write(warc_bytes)
            f_modified = f.copy()
            f_modified["actual_from_warc"] = {"len": len(warc_bytes), "md5": md5}
            f_modified["actual_in_file"] = {"len": len(warc_bytes), "md5": md5, "start_offset": all_size, "end_offset": all_size + len(warc_bytes)}
            persisted_files.append(f_modified)
            all_size += len(warc_bytes)

    print(output_file + '.jsonl')
    with open(output_file + '.jsonl', 'wt') as output_meta:
        for f_modified in persisted_files:
            output_meta.write(json.dumps(f_modified) + '\n')

    return output_file

def persist_filtered_warcs(partitions, dataset):
    from multiprocessing import Pool
    pool = Pool(2*len(partitions))
    print(f"\nPersist {len(partitions)} partitions:\n\n")
    jobs = []
    for partition in tqdm(partitions, "start partitions"):
        file_name = f"{dataset}-trec-filtered-0{partition}.warc.gz"
        output_warc_file = f"{OUT_DIR}/{dataset}/filtered/{file_name}"
        jobs.append(pool.apply_async(persist_warcs_into_file, [output_warc_file, dataset, partitions[partition]]))

    for job in jobs:
        print(job.get(timeout=60*60*5))

def step_02_persist_files(dataset):
    files = load_all_access_files(dataset)
    partitions = partition_access_files(files, dataset)
    persist_filtered_warcs(partitions, dataset)


@remote
def check_md5_of_warc_files(dataset, files):
    for f in tqdm(files):
        if "md5" in f and f["md5"]:
            continue

        bytes_in_s3 = bytes_of_warc_record_from_s3(dataset, f["source"]["file"], f["source"]["start_offset"], f["source"]["end_offset"])
        bytes_in_local = bytes_of_warc_record_from_local_file(dataset, f["file"], f["start_offset"], f["end_offset"])
        if len(bytes_in_s3) != len(bytes_in_local):
            continue
        expected_md5 = md5_sum(bytes_in_s3)
        actual_md5 = md5_sum(bytes_in_local)
        if expected_md5 != actual_md5:
            continue

        f["md5"] = expected_md5

    return files

def step_03_check_warc_records(dataset):
    files = []
    with gzip.open(f"{OUT_DIR}/{dataset}/filtered/documents.jsonl.gz", "rt") as docs_file:
        for f in docs_file:
            files.append(json.loads(f))

    streams = []
    for chunk in tqdm(chunk_array(files, 1024*2)):
        streams.append(check_md5_of_warc_files.remote(dataset, chunk))

    shutil.copy(f"{OUT_DIR}/{dataset}/filtered/documents.jsonl.gz", f"{OUT_DIR}/{dataset}/filtered/BACKUP-documents.jsonl.gz")

    with gzip.open(f"{OUT_DIR}/{dataset}/filtered/documents.jsonl.gz", "wt") as docs_file:
        for i in streams:
            for f in get(i):
                docs_file.write(json.dumps(f) + '\n')

    with_md5, without_md5 = 0, 0
    with gzip.open(f"{OUT_DIR}/{dataset}/filtered/documents.jsonl.gz", "rt") as docs_file:
        for f in docs_file:
            if "md5" in json.loads(f):
                with_md5 += 1
            else:
                without_md5 += 1
    print(f"From {with_md5 + without_md5} files, {with_md5} have the expected md5 sum, {without_md5} dont have an md5 (re-run the script to fill the gaps).")


@click.command()
@click.argument("step", type=click.Choice(["01-access-files", "02-persist-files", "03-check-warc-records"]))
@click.argument("dataset", type=click.Choice(["clueweb09", "clueweb12"]))
@click.option("--output", default=OUT_DIR, required=False)
def main(step, dataset, output):
    global OUT_DIR
    OUT_DIR = Path(output)

    if step == "01-access-files":
        print(f"Use s3 client {create_s3_client()}")
        step_01_access_files(dataset)
    elif step == "02-persist-files":
        print(f"Use s3 client {create_s3_client()}")
        step_02_persist_files(dataset)
    elif step == "03-check-warc-records":
        print(f"Use s3 client {create_s3_client()}")
        step_03_check_warc_records(dataset)
    else:
        raise ValueError(f"Unknown step '{step}'.")

if __name__ == '__main__':
    main()
