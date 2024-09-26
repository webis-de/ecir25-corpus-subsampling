#!/usr/bin/env python3
import gzip
import json
from pathlib import Path

import click
import ir_datasets
from tqdm import tqdm

PROCESSED_RESOURCES = Path(__file__).parent.parent.parent / "data" / "processed"


def load(resource):
    with gzip.open(PROCESSED_RESOURCES / Path(resource), "rt") as f:
        return json.load(f)


def load_subcorpora(dataset_id):
    leave_one_group_out_corpora = load("sampled-corpora/" + dataset_id.replace("/", "-") + ".json.gz")
    ret = {}

    for group in tqdm(leave_one_group_out_corpora, "load groups"):
        for approach in leave_one_group_out_corpora[group]:
            if approach == "complete-corpus":
                continue
            if approach not in ret:
                ret[approach] = set()
            ret[approach].update(leave_one_group_out_corpora[group][approach])
    ret = [(k, v) for k, v in ret.items()]
    return sorted(ret, key=lambda i: len(i[1]))


def resource_exists(resource):
    return (PROCESSED_RESOURCES / Path(resource)).exists()


@click.command("materialize-corpus")
@click.argument("dataset_id", type=str)
def materialize_corpus(dataset_id):
    subcorpora = load_subcorpora(dataset_id)
    dataset = ir_datasets.load("dataset_id")

    for group_name, docs in subcorpora:
        target_resource = "materialized-corpora/" + dataset_id.replace("/", "-") + f"/{group_name}/"
        if resource_exists(target_resource + '/documents.jsonl.gz'):
            continue

        documents = []
        docs_store = dataset.docs_store()

        for doc in tqdm(docs, group_name):
            documents += [{"docno": doc, "text": docs_store.load(doc).default_text(), "original_document": {}}]

        for q in ['queries.jsonl', 'queries.xml', 'topics.xml']:
            open(target_resource + '/' + q)

        with gzip.open(target_resource + '/documents.jsonl.gz', "wt") as f:
            for doc in documents:
                f.write(json.dumps(doc) + "\n")


if __name__ == "__main__":
    materialize_corpus()
