#!/usr/bin/env python3
import gzip
import json
import random

from tqdm import tqdm
from trectools import TrecRun

from corpus_subsampling.run_files import IR_DATASET_IDS, Runs

SEED = 42
DEPTH = 25000


def sample_docs(ir_datasets_id, docs):
    if ir_datasets_id.split("/")[0] not in docs:
        docs[ir_datasets_id.split("/")[0]] = set()

    docs = docs[ir_datasets_id.split("/")[0]]
    for runs in Runs(ir_datasets_id).all_runs().values():
        for r in runs:
            r = TrecRun(r)
            docs.update(r.run_data["docid"].unique())


if __name__ == "__main__":
    docs = {}
    for i in tqdm(IR_DATASET_IDS):
        sample_docs(i, docs)

    for k, v in docs.items():
        v = list(v)
        random.Random(SEED).shuffle(v)
        v = v[:DEPTH]
        with gzip.open(f"data/processed/random-documents/{k}.json.gz", "wt") as f:
            f.write(json.dumps(v))
